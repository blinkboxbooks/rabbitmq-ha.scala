package com.blinkboxbooks.hermes.rabbitmq

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.actor.Status
import akka.util.Timeout
import com.blinkbox.books.messaging._
import com.rabbitmq.client._
import java.nio.charset.{ Charset, StandardCharsets }
import org.joda.time.DateTime
import RabbitMqConsumer._
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * This actor class consumes messages from RabbitMQ and passes them on as
 * queue-independent Event messages, populated with the standard fields used
 * in the blinkbox books platform services.
 *
 * This also handles acknowledgment of messages, using the Cameo pattern.
 */
class RabbitMqConsumer(channel: Channel, queueConfig: QueueConfiguration, consumerTag: String, output: ActorRef)
  extends Actor with ActorLogging {

  channel.basicQos(queueConfig.preFetchCount)
  val newConsumer = createConsumer(channel)
  channel.queueDeclare(queueConfig.queueName, true, false, false, null)

  channel.exchangeDeclare(queueConfig.exchangeName, "topic", true)
  for (routingKey <- queueConfig.routingKeys) {
    channel.queueBind(queueConfig.queueName, queueConfig.exchangeName, routingKey)
  }

  println("Consuming!!!")
  channel.basicConsume(queueConfig.queueName, false, consumerTag, newConsumer)
  println(s"6: ${queueConfig.queueName}, false, $consumerTag, $newConsumer")

  def receive = {
    case msg: RabbitMqMessage =>
      val handler = context.actorOf(Props(new EventHandlerCameo(channel, msg.envelope.getDeliveryTag)))
      output.tell(toEvent(msg), handler)
    case msg => log.error(s"Unexpected message: $msg")
  }

  private def toEvent(msg: RabbitMqMessage): Event = {
    val timestamp = new DateTime(Option(msg.properties.getTimestamp()).getOrElse(DateTime.now))
    val contentType = Option(msg.properties.getContentType()).getOrElse(ContentType.XmlContentType.mediaType)
    val encoding = Option(msg.properties.getContentEncoding())
      .map(Charset.forName(_))
      .getOrElse(StandardCharsets.UTF_8)
    val messageId = msg.properties.getMessageId()
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

  case class QueueConfiguration(queueName: String, exchangeName: String, routingKeys: Seq[String], preFetchCount: Int)

  // Standard RabbitMQ headers used for events.
  val TransactionIdHeader = "com.blinkbox.books.transactionId"

  case class RabbitMqMessage(deliveryTag: Long, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte])

  /**
   * Actor whose sole responsibility is to ack or nack a single message, then stop.
   */
  private class EventHandlerCameo(channel: Channel, deliveryTag: Long) extends Actor with ActorLogging {

    def receive = {
      case Status.Success(_) => {
        log.debug(s"ACKing message $deliveryTag")
        if (Try(channel.basicAck(deliveryTag, false)).isFailure)
          log.warning(s"Faile to ACK message $deliveryTag")
        context.stop(self)
      }
      case Status.Failure(e) =>
        log.warning(s"NACKing message $deliveryTag")
        if (Try(channel.basicNack(deliveryTag, false, false)).isFailure)
          log.warning(s"Faile to ACK message $deliveryTag")
        context.stop(self)
    }
  }

}
