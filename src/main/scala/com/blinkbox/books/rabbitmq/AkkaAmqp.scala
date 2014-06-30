package com.blinkbox.books.rabbitmq

import com.rabbitmq.client._
import com.blinkbox.books.rabbitmq.RabbitMqReliablePublisher._
import akka.actor.{ActorLogging, ActorRef, Actor}
import akka.pattern.ask
import akka.util.Timeout
import scala.util.{ Try, Success, Failure }
import scala.collection.immutable.{HashMap, TreeSet}
import scala.concurrent.duration._

/**
 * Data of an AMQP message.
 */
case class Message(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte])

object AmqpConsumerActor {
  def apply(channel: Channel,  actor: ActorRef, queueName: String, exchangeName: Option[String],
  amqpTimeout: Timeout, routingKeys: Option[Seq[String]], consumerTag: String, preFetchCount: Int) =
    new AmqpConsumerActor(channel,  actor, queueName, exchangeName, amqpTimeout, routingKeys, consumerTag, preFetchCount)
}

/**
 * Actor that consumes messages from a configured AMQP queue and passes them on to an output actor.
 */
class AmqpConsumerActor(channel: Channel, val actor: ActorRef, queueName: String, exchangeName: Option[String],
                        amqpTimeout: Timeout, routingKeys: Option[Seq[String]], consumerTag: String, preFetchCount: Int)
  extends Actor with ActorLogging {

  implicit val timeout = amqpTimeout
  implicit val ec = context.dispatcher

  channel.basicQos(preFetchCount)
  val newConsumer = createConsumer(channel)
  channel.queueDeclare(queueName, true, false, false, null)

  if (exchangeName != None && routingKeys != None) {
    channel.exchangeDeclare(exchangeName.get, "topic", true)
    for (routingKey <- routingKeys.get) {
      channel.queueBind(queueName, exchangeName.get, routingKey)
    }
  }

  channel.basicConsume(queueName, false, consumerTag, newConsumer)

  override def receive = {
    // Pass on message to our output, and ACK/NACK inbound message on response.
    case message @ Message(_, envelope, _, _) => {
      val response = actor ? message
      val delTag = envelope.getDeliveryTag
      response.onComplete {
        case Success(_) => {
          log.debug(s"Acking message with delivery tag $delTag")
          channel.basicAck(envelope.getDeliveryTag, false)
        }
        case Failure(e) =>
          log.info(s"Rejecting message with delivery tag $delTag: ${e.getMessage}")
          channel.basicNack(envelope.getDeliveryTag, false, false)
      }
    }
  }

  /**
   * Create a message consumer that will pick up AMQP messages and pass them to the inbox of this actor.
   */
  private def createConsumer(channel: Channel): Consumer =
    new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope,
                                  properties: AMQP.BasicProperties, body: Array[Byte]) = {
        log.debug(s"Forwarding message with consumer tag $consumerTag")
        self ! Message(consumerTag, envelope, properties, body)
      }
    }
}

/**
 * Actor that publishes incoming messages to a configured AMQP exchange.
 */
object RabbitMqReliablePublisher {

  def apply(channel: Channel, queueName: String, amqpTimeout: Timeout) =
    new RabbitMqReliablePublisher(channel, queueName, amqpTimeout) with AmqpRetry

  case class PublishRequest(msg: Message, attempts: Int = 0)

  protected case class RabbitMessage(seqNo: Long, message: Message) extends Ordered[RabbitMessage] {
    override def compare(that: RabbitMessage): Int = this.seqNo compare that.seqNo
  }

  private case class AckMessage(seqNo: Long, multiple: Boolean)

  private case class NackMessage(seqNo: Long, multiple: Boolean)
}

trait AmqpRetry {
  val RetryFactor = 1
  val MaxAttempts = 4
}

class RabbitMqReliablePublisher(channel: Channel, queueName: String, amqpTimeout: Timeout) extends Actor with ActorLogging {
  this: AmqpRetry =>

  implicit val timeout = amqpTimeout
  implicit val ec = context.dispatcher

  private var unconfirmedSet = TreeSet.empty[RabbitMessage]
  private var attemptsMap = HashMap.empty[Long, Int]

  channel.queueDeclare(queueName, true, false, false, null)

  // enable RabbitMQ Confirms
  channel.confirmSelect()
  channel.addConfirmListener(new ConfirmListener {
    override def handleAck(seqNo: Long, multiple: Boolean) {
      self ! AckMessage(seqNo, multiple)
    }
    override def handleNack(seqNo: Long, multiple: Boolean) {
      log.warning(s"Received NACK with seqNo #$seqNo (multiple:$multiple)")
      self ! NackMessage(seqNo, multiple)
    }
  })

  def unconfirmedMessages = unconfirmedSet

  override def receive = {
    case PublishRequest(msg, attempts) =>
      // add this message to unconfirmed messages, initially
      val seqNo = channel.getNextPublishSeqNo
      unconfirmedSet += RabbitMessage(seqNo, msg)
      attemptsMap += (seqNo -> (attempts + 1))

      // making sure that the messages are persistent
      val deliveryMode = MessageProperties.MINIMAL_PERSISTENT_BASIC.getDeliveryMode
      val properties = msg.properties.builder().deliveryMode(deliveryMode).build()

      // send the message and account for failure
      Try(channel.basicPublish("", queueName, properties, msg.body)) match {
        case Success(_) => log.debug(s"Published message to queue '$queueName'")
        case Failure(e) =>
          log.error(s"Failed to publish message $seqNo to queue '$queueName': $e")
          self ! NackMessage(seqNo, multiple = false)
      }

    case AckMessage(seqNo, multiple) =>
      // remove the respective messages from the unconfirmed set
      unconfirmedSet = if (multiple) unconfirmedSet dropWhile { _.seqNo <= seqNo }
      else unconfirmedSet find { _.seqNo == seqNo } match {
        case Some(msg) => unconfirmedSet - msg
        case None => unconfirmedSet
      }
      // remove it from the hash map
      attemptsMap -= seqNo

    case NackMessage(seqNo, multiple) =>
      // check which messages we need to resend
      val toResend =
        if (multiple) unconfirmedSet takeWhile { _.seqNo <= seqNo  }
        else (unconfirmedSet find { _.seqNo == seqNo }).toList
      // remove them from the unconfirmed
      unconfirmedSet --= toResend
      // resend messages
      toResend foreach { m =>
        attemptsMap get m.seqNo match {
          case Some(attempts) =>
            if (attempts < MaxAttempts) {
              log.info(s"Resending message with seqNo #${m.seqNo} and retry factor $RetryFactor (${attempts + 1}/$MaxAttempts)")
              context.system.scheduler.scheduleOnce(((attempts - 1) * RetryFactor).seconds, self, PublishRequest(m.message, attempts))
            }
            else {
              log.error(s"Message seqNo #${m.seqNo} failed to be delivered after $attempts attempts")
            }
            attemptsMap -= m.seqNo
          case None => log.error(s"Could not find data for this seqNo #${m.seqNo} (multiple:$multiple)")
        }
      }
  }
}
