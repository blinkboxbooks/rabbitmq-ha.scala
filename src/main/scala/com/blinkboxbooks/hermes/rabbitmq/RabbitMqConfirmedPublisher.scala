package com.blinkboxbooks.hermes.rabbitmq

import akka.actor.{ Actor, ActorLogging, ActorRef }
import akka.actor.Status.{ Status, Success, Failure }
import akka.util.Timeout
import com.blinkbox.books.messaging.{ ContentType, Event }
import com.rabbitmq.client._
import com.rabbitmq.client.AMQP.BasicProperties
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/**
 * This actor class will publish messages to a RabbitMQ topic exchange, publishing them as persistent
 * messages, and using publisher confirms to get reliable confirmation when messages have been
 * successfully processed by a receiver. See [[https://www.rabbitmq.com/confirms.html]] for details
 * on what level of guarantees this provides.
 *
 * When publishing succeeds, a Success message is sent to the sender of the original request.
 *
 * When publishing fails, either when a negative confirmation is received, or the timeout is reached,
 * a Failure is sent to the sender.
 *
 * This class does NOT retry sending of messages. Hence it is suitable when the message being processed
 * can be re-processed at its origin, e.g. when the upstream message is already coming from a persistent queue,
 * or other persistent storage.
 *
 */
class RabbitMqConfirmedPublisher(channel: Channel, exchange: String, routingKey: String, messageTimeout: FiniteDuration)
  extends Actor with ActorLogging {

  import RabbitMqConfirmedPublisher._
  import context.dispatcher

  // Tracks sequence numbers of messages that haven't been confirmed yet, and who to tell about the result.
  private[rabbitmq] var pendingMessages = Map[Long, ActorRef]()

  // Declare the exchange we'll publish to, as a durable topic exchange.
  channel.exchangeDeclare(exchange, "topic", true)

  // Enable RabbitMQ Publisher Confirms.
  channel.confirmSelect()

  // Callback for publisher confirm events.
  channel.addConfirmListener(new ConfirmListener {
    override def handleAck(seqNo: Long, multiple: Boolean) {
      self ! Ack(seqNo, multiple)
    }
    override def handleNack(seqNo: Long, multiple: Boolean) {
      self ! Nack(seqNo, multiple)
    }
  })

  override def receive = {
    case PublishRequest(event) =>
      val seqNo = channel.getNextPublishSeqNo
      publishMessage(seqNo, event) match {
        case util.Success(_) =>
          log.debug(s"Published message $seqNo to exchange '$exchange' with routing key '$routingKey'")
          context.system.scheduler.scheduleOnce(messageTimeout, self, TimedOut(seqNo))
          pendingMessages += seqNo -> sender
        case util.Failure(e) =>
          log.error(e, s"Failed to publish message $seqNo to exchange '$exchange' with routing key '$routingKey'")
          sender ! Failure(new PublishException("Failure when trying to publish message", e))
      }
    case Ack(seqNo, multiple) => updateConfirmedMessages(seqNo, multiple, Success())
    case Nack(seqNo, multiple) => updateConfirmedMessages(seqNo, multiple, nackFailure)
    case TimedOut(seqNo) => updateConfirmedMessages(seqNo, false, timeoutFailure)
  }

  private def publishMessage(seqNo: Long, event: Event) =
    Try(channel.basicPublish(exchange, routingKey, propertiesForEvent(event), event.body.content))

  private val nackFailure = Failure(new PublishException("Message not successfully received"))
  private val timeoutFailure = Failure(new PublishException(s"Message timed out after $messageTimeout"))

  /**
   * Find the messages affected by the ACK/NACK, send the response to them, and remove them from
   * the collection of pending messages.
   */
  private def updateConfirmedMessages(seqNo: Long, multiple: Boolean, response: Status) {
    log.debug(s"Received update for message $seqNo (multiple=$multiple): $response")
    val (confirmed, remaining) = pendingMessages.partition(isAffectedByConfirmation(seqNo, multiple))
    confirmed.foreach(entry => entry._2 ! response)
    pendingMessages = remaining
  }

  /** Predicate for deciding whether a pending message is affected by a given ACK/NACK or not. */
  private def isAffectedByConfirmation(seqNo: Long, multiple: Boolean)(entry: (Long, ActorRef)): Boolean =
    if (multiple) entry._1 <= seqNo else entry._1 == seqNo

  /** Convert Event metadata to RabbitMQ message properties. */
  private def propertiesForEvent(event: Event): BasicProperties = {
    // Required properties.
    val builder = new BasicProperties.Builder()
      .deliveryMode(MessageProperties.MINIMAL_PERSISTENT_BASIC.getDeliveryMode)
      .messageId(event.header.id)
      .timestamp(event.header.timestamp.toDate)
      .appId(event.header.originator)
      .contentType(ContentType.XmlContentType.mediaType)

    // Optional properties.
    event.header.userId.foreach { userId => builder.userId(userId) }
    event.header.transactionId.foreach { transactionId =>
      val headers = Map[String, Object](RabbitMqConsumer.TransactionIdHeader -> transactionId)
      builder.headers(headers.asJava)
    }
    event.body.contentType.charset.foreach { charset => builder.contentEncoding(charset.name) }

    builder.build()
  }

}

object RabbitMqConfirmedPublisher {

  /** Message used for triggering publishing of event. */
  case class PublishRequest(event: Event)

  /** Exception that may be returned in failure respones from actor. */
  case class PublishException(message: String, cause: Throwable = null) extends Exception(message, cause)

  private case class Ack(seqNo: Long, multiple: Boolean)
  private case class Nack(seqNo: Long, multiple: Boolean)
  private case class TimedOut(seqNo: Long)
}
