package com.blinkbox.books.rabbitmq

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.actor.Status.{ Status, Success, Failure }
import com.blinkbox.books.messaging.{ ContentType, Event }
import com.rabbitmq.client._
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.config.Config
import java.io.IOException
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.util.Try

import RabbitMqConfirmedPublisher._

/**
 * This actor class will publish events to a RabbitMQ topic exchange, publishing them as persistent
 * messages, and using publisher confirms to get reliable confirmation when messages have been
 * successfully processed by a receiver, and notifying clients about the outcome of publishing a message.
 * See [[https://www.rabbitmq.com/confirms.html]] for details on what level of guarantees this provides.
 *
 * To publish an Event, send the event object to this actor.
 *
 * When publishing succeeds, a Success message is sent to the sender of the original request.
 *
 * When publishing fails, due to either a negative confirmation or a timeout,
 * a Failure is sent to the sender.
 *
 * This class does NOT retry sending of messages. Hence it is suitable when the message being processed
 * can be re-processed at its origin, e.g. when the upstream message is already coming from a persistent queue,
 * or other persistent storage.
 *
 * @param connection This is the RabbitMQ connection that messages will be published on. The actor
 * will be publishing messages concurrently on multiple threads, hence will create Channels to publish
 * the messages on itself (publisher confirms don't work correctly if multiple threads publish to a single Channel,
 * see com.rabbitmq.client.Channel).
 *
 * @param config Settings that describe what how messages are published. Any exchange or queue specified here
 * will be declared by this actor on startup.
 *
 */
class RabbitMqConfirmedPublisher(connection: Connection, config: PublisherConfiguration)
  extends Actor with ActorLogging {

  import context.dispatcher

  private val exchangeName = config.exchange getOrElse ""

  // Initialise exchanges/queues.
  initConnection()

  override def receive = {
    case event: Event =>
      val originator = sender
      val singleMessagePublisher = context.actorOf(
        Props(new SingleEventPublisher(createChannel(), originator, exchangeName, config.routingKey, config.messageTimeout))
          .withDispatcher("event-publisher-dispatcher"),
        name = s"msg-publisher-for-${event.header.id}")
      singleMessagePublisher ! event

    case msg => log.error(s"Unexpected message received: $msg")
  }

  /** Ensure the required queues, exchanges and bindings are present. */
  private def initConnection() {
    val channel = createChannel()
    try
      // Either declare exchange or queue, depending on what we're publishing to.
      config.exchange match {
        case Some(name) =>
          channel.exchangeDeclare(name, "topic", true)
          log.debug(s"Declared topic exchange $name, used as the exchange to publish to")
        case None =>
          channel.queueDeclare(config.routingKey, true, false, false, null)
          log.debug(s"Declared queue ${config.routingKey}, used as the queue to publish directly to")
      }
    finally channel.close()
  }

  /** Create a Channel with Publisher Confirms enabled. */
  private def createChannel(): Channel = {
    val channel = connection.createChannel()
    // Enable RabbitMQ Publisher Confirms.
    channel.confirmSelect()
    channel
  }

}

object RabbitMqConfirmedPublisher {

  /**
   * Settings for publisher.
   *
   * @param exchange The name of the exchange to publish messages to. If set to None, this actor will publish
   * messages to the "default exchange", which in RabbitMQ means publishing directly to a queue, with the routing key
   * being the name of the queue.
   *
   * @param messageTimeout The timeout for each published message, i.e. the time at which the client will receive
   * a failure notification for a message if confirmation hasn't been received for this.
   *
   */
  case class PublisherConfiguration(exchange: Option[String], routingKey: String, messageTimeout: FiniteDuration)
  object PublisherConfiguration {
    def apply(config: Config): PublisherConfiguration = {
      val exchange = if (config.hasPath("exchangeName")) Some(config.getString("exchangeName")) else None
      val routingKey = config.getString("routingKey")
      val messageTimeout = config.getDuration("messageTimeout", TimeUnit.SECONDS).seconds
      PublisherConfiguration(exchange, routingKey, messageTimeout)
    }
  }

  /** Exception that may be returned in failure responses from actor. */
  case class PublishException(message: String, cause: Throwable = null) extends IOException(message, cause)

  private case class Ack(seqNo: Long, multiple: Boolean)
  private case class Nack(seqNo: Long, multiple: Boolean)
  private case object TimedOut

  /**
   * Helper class that's responsible for publishing a single event.
   * Note that this performs blocking operations on the RabbitMQ API. This API is very hard to
   * use in an asynchronous, non-blocking way, sadly, especially when using publisher confirms.
   */
  private class SingleEventPublisher(channel: Channel, originator: ActorRef,
    exchange: String, routingKey: String, timeout: FiniteDuration)
    extends Actor with ActorLogging {

    import context.dispatcher

    // Register callback for confirmations.
    channel.addConfirmListener(new ConfirmListener {
      override def handleAck(seqNo: Long, multiple: Boolean) {
        self ! Ack(seqNo, multiple)
      }
      override def handleNack(seqNo: Long, multiple: Boolean) {
        self ! Nack(seqNo, multiple)
      }
    })

    def receive = {
      case event: Event =>
        context.system.scheduler.scheduleOnce(timeout, self, TimedOut)
        Try(publishMessage(event)) match {
          case util.Failure(e) => complete(publishFailure(e))
          case util.Success(_) => // OK
        }
      case Ack(seqNo, multiple) => complete(Success())
      case Nack(seqNo, multiple) => complete(nackFailure)
      case TimedOut => complete(timeoutFailure(timeout))
      case msg => log.error(s"Unexpected message: $msg")
    }

    /** The actor is given its own Channel so it's essential we close this when we're done. */
    override def postStop() = channel.close()

    private def publishMessage(event: Event) = blocking {
      // Note: Lyra can make the basicPublish() method blocking (e.g. when the broker connection is down),
      // so this needs to be inside the blocking{} block.
      channel.basicPublish(exchange, routingKey, propertiesForEvent(event), event.body.content)
      log.debug(s"Published message with ID ${event.header.id} with routing key '$routingKey'")
    }

    private def complete(response: Status) {
      originator ! response
      context.stop(self)
    }

    private def publishFailure(e: Throwable) = Failure(PublishException(s"Failed to publish message", e))
    private def timeoutFailure(timeout: Duration) = Failure(PublishException(s"Message timed out after $timeout"))
    private val nackFailure = Failure(PublishException("Message not successfully received"))

    /** Convert Event metadata to RabbitMQ message properties. */
    private def propertiesForEvent(event: Event): BasicProperties = {
      // Required properties.
      val builder = new BasicProperties.Builder()
        .deliveryMode(MessageProperties.MINIMAL_PERSISTENT_BASIC.getDeliveryMode)
        .messageId(event.header.id)
        .timestamp(event.header.timestamp.toDate)
        .appId(event.header.originator)
        .contentType(event.body.contentType.mediaType)

      // Optional properties.
      val userIdHeader = event.header.userId map { userId => (RabbitMqConsumer.UserIdHeader -> userId) }
      val transactionIdHeader = event.header.transactionId.map { transactionId => (RabbitMqConsumer.TransactionIdHeader -> transactionId) }

      val allHeaders: Map[String, Object] = List(userIdHeader, transactionIdHeader).flatten.toMap
      builder.headers(allHeaders.asJava)

      event.body.contentType.charset.foreach { charset => builder.contentEncoding(charset.name) }

      builder.build()
    }

  }

}

