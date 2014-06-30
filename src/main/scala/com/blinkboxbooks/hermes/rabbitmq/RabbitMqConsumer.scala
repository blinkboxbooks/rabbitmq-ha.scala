package com.blinkboxbooks.hermes.rabbitmq

import akka.actor.{ Actor, ActorLogging, ActorRef, Props, Status }
import akka.actor.Status.{ Success, Failure }
import akka.util.Timeout
import com.blinkbox.books.messaging._
import com.rabbitmq.client._
import com.typesafe.config.Config
import java.nio.charset.{ Charset, StandardCharsets }
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import scala.util.Try

import RabbitMqConsumer._

/**
 * This actor class consumes messages from RabbitMQ topic exchanges bound to a queue,
 * and passes them on as Rabbit-MQ independent Event messages, populated with the standard fields used
 * in the blinkbox books platform services.
 *
 * This also handles acknowledgment of messages, using the Cameo pattern.
 * The output actors that messages are forwarded to are responsible for responding with a Success or Failure
 * so that the incoming message can be acked or nacked.
 *
 * This class assumes the given Channel is reliable, so will not try to reconnect channels on failure.
 * Hence it should be used with a library that provides such reliable channels, e.g. Lyra.
 *
 * This class will not handle retrying in the case of failure scenarios, any such behaviour has to
 * be implemented in downstream actors if desired.
 *
 */
class RabbitMqConsumer(channel: Channel, queueConfig: QueueConfiguration, consumerTag: String, output: ActorRef)
  extends Actor with ActorLogging {

  def receive = initialising

  def initialising: Receive = {
    case Init =>
      init()
      context.become(initialised)
      sender ! Status.Success("Initialised")
    case msg => log.error(s"Unexpected message in uninitialised consumer: $msg")
  }

  def initialised: Receive = {
    case msg: RabbitMqMessage =>
      val handler = context.actorOf(Props(new EventHandlerCameo(channel, msg.envelope.getDeliveryTag)))
      toEvent(msg) match {
        case util.Success(event) => output.tell(event, handler)
        case util.Failure(e) => handleInvalidMessage(msg, e)
      }
    case msg => log.error(s"Unexpected message in initialised consumer: $msg")
  }

  /**
   *  Deal with incoming message that can't be converted to a valid Event.
   *  The current policy for this is:
   *
   *  - NACK it to RabbitMQ without requeuing it (to avoid loops, and to enable
   *  RabbitMQ dead-letter handling of the message.
   *  - Log it for manual inspection.
   */
  def handleInvalidMessage(msg: RabbitMqMessage, e: Throwable) = {
    val deliveryTag = msg.envelope.getDeliveryTag
    if (Try(channel.basicNack(deliveryTag, false, false)).isFailure)
      log.warning(s"Failed to NACK message $deliveryTag")
    log.error(e, s"Received invalid message:\n$msg")
  }

  private def init() {
    log.debug("Initialising RabbitMQ channel")
    channel.basicQos(queueConfig.prefetchCount)
    val newConsumer = createConsumer(channel)
    channel.queueDeclare(queueConfig.queueName, true, false, false, null)

    channel.exchangeDeclare(queueConfig.exchangeName, "topic", true)
    for (routingKey <- queueConfig.routingKeys) {
      channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, routingKey)
    }

    channel.basicConsume(queueConfig.queueName, false, consumerTag, newConsumer)
    log.debug("RabbitMQ channel initialised")
  }

  private def toEvent(msg: RabbitMqMessage): Try[Event] = Try {
    val timestamp = new DateTime(Option(msg.properties.getTimestamp()).getOrElse(throw new IllegalArgumentException("Missing timestamp in event")))
    val contentType = Option(msg.properties.getContentType()).getOrElse(ContentType.XmlContentType.mediaType)
    val encoding = Option(msg.properties.getContentEncoding())
      .map(Charset.forName(_))
      .getOrElse(StandardCharsets.UTF_8)
    val messageId = Option(msg.properties.getMessageId()).getOrElse("unknown") // To cope with legacy messages.
    val userId = Option(msg.properties.getUserId())
    val transactionId = for (
      headers <- Option(msg.properties.getHeaders);
      txId <- headers.asScala.get(TransactionIdHeader)
    ) yield txId.toString
    // TBD: val flowId = Option(msg.properties.getCorrelationId())

    val originator = Option(msg.properties.getAppId()).getOrElse("unknown") // To cope with legacy messages.
    Event(EventHeader(messageId, new DateTime(timestamp), originator, userId, transactionId),
      EventBody(msg.body, ContentType(contentType, Some(encoding))))
  }

  /**
   *  Create a RabbitMQ API message consumer that will pick up AMQP messages and pass them to this actor.
   */
  private def createConsumer(channel: Channel): Consumer = new DefaultConsumer(channel) {
    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) = {
      val deliveryTag = envelope.getDeliveryTag
      log.debug(s"Forwarding message $deliveryTag from consumer '$consumerTag'")
      self ! RabbitMqMessage(deliveryTag, envelope, properties, body)
    }
  }

}

object RabbitMqConsumer {

  case object Init
  case class QueueConfiguration(queueName: String, exchangeName: String, routingKeys: Seq[String], prefetchCount: Int)

  object QueueConfiguration {
    def apply(config: Config): QueueConfiguration = {
      val queueName = config.getString("queueName")
      val exchangeName = config.getString("exchangeName")
      val routingKeys = config.getStringList("routingKeys").asScala.toList
      val prefetchCount = config.getInt("prefetchCount")
      QueueConfiguration(queueName, exchangeName, routingKeys, prefetchCount)
    }
  }

  // Standard RabbitMQ headers used for events.
  val TransactionIdHeader = "com.blinkbox.books.transactionId"

  case class RabbitMqMessage(deliveryTag: Long, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte])

  /**
   * Actor whose sole responsibility is to ack or nack a single message, then stop.
   */
  private class EventHandlerCameo(channel: Channel, deliveryTag: Long) extends Actor with ActorLogging {

    def receive = {
      case Success(_) =>
        log.debug(s"ACKing message $deliveryTag")
        if (Try(channel.basicAck(deliveryTag, false)).isFailure)
          log.warning(s"Failed to ACK message $deliveryTag")
        context.stop(self)
      case Failure(e) =>
        log.warning(s"NACKing message $deliveryTag: ${e.getMessage}")
        if (Try(channel.basicNack(deliveryTag, false, false)).isFailure)
          log.warning(s"Failed to NACK message $deliveryTag")
        context.stop(self)
      case msg =>
        log.warning(s"Unexpected message: $msg")
    }
  }

}
