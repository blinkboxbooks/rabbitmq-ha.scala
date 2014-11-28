package com.blinkbox.books.rabbitmq

import java.nio.charset.Charset

import akka.actor.Status.{Failure, Success}
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import com.blinkbox.books.config.RichConfig
import com.blinkbox.books.messaging._
import com.blinkbox.books.rabbitmq.RabbitMqConsumer._
import com.rabbitmq.client._
import com.typesafe.config.Config
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.util.Try

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
      Try(init()) match {
        case scala.util.Failure(e) =>
          log.error(e, "Failed to initialise")
          sender ! Status.Failure(e)
        case _ =>
          context.become(initialised)
          log.info("Initialised")
          sender ! Status.Success("Initialised")
      }

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

  private def init(): Unit = {
    log.info("Initialising RabbitMQ channel")
    channel.basicQos(queueConfig.prefetchCount)
    val newConsumer = createConsumer(channel)

    channel.queueDeclare(queueConfig.queueName, true, false, false, null)
    log.debug(s"Declared queue ${queueConfig.queueName}")

    channel.exchangeDeclare(queueConfig.exchangeName, queueConfig.exchangeType, true)
    log.debug(s"Declared durable ${queueConfig.exchangeType} exchange '${queueConfig.exchangeName}'")

    // Bind queue to the exchange.
    queueConfig.exchangeType match {
      case "fanout" =>
        channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, "")
        log.debug(s"Bound queue ${queueConfig.queueName} to fanout exchange ${queueConfig.exchangeName}")
      case "topic" =>
        for (routingKey <- queueConfig.routingKeys) {
          channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, routingKey)
          log.debug(s"Bound queue ${queueConfig.queueName} to topic exchange ${queueConfig.exchangeName}, with routing key $routingKey")
        }
      case "headers" | "match" =>
        queueConfig.bindingArguments.foreach { bindingArguments =>
          channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, "", bindingArguments.asJava)
          log.debug(s"Bound queue ${queueConfig.queueName} to header exchange ${queueConfig.exchangeName} with bindings $bindingArguments")
        }
    }

    channel.basicConsume(queueConfig.queueName, false, consumerTag, newConsumer)
    log.info("RabbitMQ channel initialised")
  }

  private def toEvent(msg: RabbitMqMessage): Try[Event] = Try {
    val timestamp = new DateTime(Option(msg.properties.getTimestamp).getOrElse(DateTime.now)) // To cope with legacy messages.
    val mediaType = Option(msg.properties.getContentType).map(MediaType(_)).getOrElse(ContentType.XmlContentType.mediaType)

    val encoding = Option(msg.properties.getContentEncoding).map(Charset.forName)
    val messageId = Option(msg.properties.getMessageId).getOrElse(EventHeader.generateId()) // To cope with legacy messages.
    // TBD: val flowId = Option(msg.properties.getCorrelationId())

    val headers = Option(msg.properties.getHeaders).map(_.asScala)
    val transactionId = headers.flatMap(_.get(TransactionIdHeader)).map(_.toString)
    val userId = headers.flatMap(_.get(UserIdHeader)).map(_.toString)
    val additional = headers.getOrElse(Map[String, String]()).collect {
      case (k: String, v: String) => (k, v)
    }.toMap

    val originator = Option(msg.properties.getAppId).getOrElse("unknown") // To cope with legacy messages.
    Event(
      EventHeader(messageId, new DateTime(timestamp), originator, userId, transactionId, additional),
      EventBody(msg.body, ContentType(mediaType, encoding))
    )
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
  case class QueueConfiguration(queueName: String, exchangeName: String, exchangeType: String,
    routingKeys: Seq[String], bindingArguments: List[Map[String, AnyRef]], prefetchCount: Int) {

    if (!Set("fanout", "topic", "headers", "match").contains(exchangeType))
      throw new IllegalArgumentException(s"Illegal exchange type '$exchangeType'")

    // Check bindingArguments and routingKey mutual exclusion.
    if (routingKeys.nonEmpty && bindingArguments.nonEmpty)
      throw new IllegalArgumentException("bindingArguments and routingKey are mutually exclusive")

    // Check that binding arguments are specified if we use a header exchange.
    if ((exchangeType == "headers" || exchangeType == "match") && bindingArguments.isEmpty)
      throw new IllegalArgumentException("Must specify binding arguments for header exchange")

    if (exchangeType == "topic" && routingKeys.isEmpty) {
      throw new IllegalArgumentException("Must provide at least one routing key for topic exchange")
    }

  }

  object QueueConfiguration {
    def apply(config: Config): QueueConfiguration = {
      val queueName = config.getString("queueName")
      val exchangeName = config.getString("exchangeName")
      val exchangeType = config.getString("exchangeType")
      val routingKeys = if (config.hasPath("routingKeys")) config.getStringList("routingKeys").asScala.toList else List()
      val prefetchCount = config.getInt("prefetchCount")
      val bindingArgs = config.getListOption("bindingArguments").flatMap(f => Option(f.map(_.unwrapped.asScala.toMap)))
      QueueConfiguration(queueName, exchangeName, exchangeType, routingKeys, bindingArgs.getOrElse(List()), prefetchCount)
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
