package com.blinkbox.books.rabbitmq


import akka.actor.{ Actor, ActorLogging, ActorRef, Props, Status }
import akka.actor.Status.{ Success, Failure }
import com.blinkbox.books.messaging._
import com.rabbitmq.client._
import com.typesafe.config.{ Config}
import java.nio.charset.{ Charset  }
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Try
import com.blinkbox.books.config.RichConfig

import RabbitMqConsumer._

/**
 * This actor class consumes messages from RabbitMQ topic exchanges bound to a queue,
 * and passes them on as Rabbit-MQ independent Event messages, populated with the standard fields used
 * in the blinkbox books platform services.
 *
 * This also handles acknowledgment of messages, using the Cameo pattern.
 * The output actors that messages are forwarded to are responsible for responding with a Success or Failure
 * so that the incoming message can be acked or rejected.
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
   *  - Reject it to RabbitMQ without re-queuing it (to avoid loops, and to enable
   *  RabbitMQ dead-letter handling of the message).
   *  - Log it for manual inspection.
   */
  def handleInvalidMessage(msg: RabbitMqMessage, e: Throwable) = {
    val deliveryTag = msg.envelope.getDeliveryTag
    if (Try(channel.basicReject(deliveryTag, false)).isFailure)
      log.warning(s"Failed to reject message $deliveryTag")
    log.error(e, s"Received invalid message:\n$msg")
  }

  private def init() {
    log.info("Initialising RabbitMQ channel")
    channel.basicQos(queueConfig.prefetchCount)
    val newConsumer = createConsumer(channel)

    channel.queueDeclare(queueConfig.queueName, true, false, false, null)
    log.debug(s"Declared queue ${queueConfig.queueName}")

    // Only declare a topic exchange if at least one routing key is given.
    // This allows legacy services that use manually created fanout exchanges to work OK.
    if (queueConfig.routingKeys.nonEmpty) {
      channel.exchangeDeclare(queueConfig.exchangeName, "topic", true)
      log.debug(s"Declared topic exchange ${queueConfig.exchangeName}")
    }

    // Binding queues to the exchange using routing keys - or a single empty routing key
    // if the exchange isn't a topic exchange.
    val boundRoutingKeys = if (queueConfig.routingKeys.nonEmpty) queueConfig.routingKeys else Seq()
    for (routingKey <- boundRoutingKeys) {
      channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, routingKey)
      log.debug(s"Bound queue ${queueConfig.queueName} to exchange ${queueConfig.exchangeName}, with routing key $routingKey")
    }

    if (queueConfig.headerArgs.nonEmpty) {
      channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, "", queueConfig.headerArgs)
    }

    channel.basicConsume(queueConfig.queueName, false, consumerTag, newConsumer)
    log.info("RabbitMQ channel initialised")
  }

  private def toEvent(msg: RabbitMqMessage): Try[Event] = Try {
    val timestamp = new DateTime(Option(msg.properties.getTimestamp).getOrElse(DateTime.now)) // To cope with legacy messages.
    val mediaType = Option(msg.properties.getContentType).map(MediaType(_)).getOrElse(ContentType.XmlContentType.mediaType)
    val encoding = Option(msg.properties.getContentEncoding)
      .map(Charset.forName)
    val messageId = Option(msg.properties.getMessageId).getOrElse("unknown") // To cope with legacy messages.
    // TBD: val flowId = Option(msg.properties.getCorrelationId())

    val headers = Option(msg.properties.getHeaders).map(_.asScala)
    val transactionId = headers.flatMap(_.get(TransactionIdHeader)).map(_.toString)
    val userId = headers.flatMap(_.get(UserIdHeader)).map(_.toString)

    val originator = Option(msg.properties.getAppId).getOrElse("unknown") // To cope with legacy messages.
    Event(EventHeader(messageId, new DateTime(timestamp), originator, userId, transactionId),
      EventBody(msg.body, ContentType(mediaType, encoding)))
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
  case class QueueConfiguration(queueName: String, exchangeName: String, routingKeys: Seq[String], headerArgs : Map[String, AnyRef], prefetchCount: Int)

  object QueueConfiguration {
    def apply(config: Config): QueueConfiguration = {
      val queueName = config.getString("queueName")
      val exchangeName = config.getString("exchangeName")
      val routingKeys = config.getStringList("routingKeys").asScala.toList
      val prefetchCount = config.getInt("prefetchCount")
      val bindingArgs =  config.getConfigObjectOption("bindingArguments")

      val mapArgs =bindingArgs.flatMap( f => Option(f.unwrapped().asScala.toMap)) //mutable to immutable map

      //check bindingArguments and routingKey mutual exclusion
      if (routingKeys.nonEmpty && bindingArgs.nonEmpty)
        throw new IllegalArgumentException("bindingArguments and routingKey must be mutually exclusive")
      QueueConfiguration(queueName, exchangeName, routingKeys, mapArgs.getOrElse(Map()), prefetchCount)
    }
  }

  // Standard RabbitMQ headers used for events.
  val TransactionIdHeader = "com.blinkbox.books.transactionId"
  val UserIdHeader = "com.blinkbox.books.userId"

  case class RabbitMqMessage(deliveryTag: Long, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte])

  /**
   * Actor whose sole responsibility is to ack or reject a single message, then stop.
   */
  private class EventHandlerCameo(channel: Channel, deliveryTag: Long) extends Actor with ActorLogging {

    def receive = {
      case Success(_) =>
        log.debug(s"acking message $deliveryTag")
        if (Try(channel.basicAck(deliveryTag, false)).isFailure)
          log.warning(s"Failed to ack message $deliveryTag")
        context.stop(self)
      case Failure(e) =>
        log.warning(s"Rejecting message $deliveryTag: ${e.getMessage}")
        if (Try(channel.basicReject(deliveryTag, false)).isFailure)
          log.warning(s"Failed to reject message $deliveryTag")
        context.stop(self)
      case msg =>
        log.warning(s"Unexpected message: $msg")
    }
  }
}
