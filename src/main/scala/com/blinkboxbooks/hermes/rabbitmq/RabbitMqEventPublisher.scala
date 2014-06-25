package com.blinkboxbooks.hermes.rabbitmq

import com.blinkbox.books.messaging._
import com.rabbitmq.client.Channel
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

/**
 * Simple message publisher for RabbitMQ.
 *
 * This publishes events without retries, but using persistent messages and publisher confirms.
 * Hence this is suitable for clients that want to publish messages in a reliable way, but would
 * rather deal with failures itself instead of having requests retried for them.
 *
 * The Future returned by the publish() method will only complete when it has had confirmation that
 * publishing succeeded (to the extend RabbitMQ can guarantee this).
 *
 */
class RabbitMqEventPublisher(channel: Channel, exchangeName: String, routingKey: String)(implicit ec: ExecutionContext)
  extends EventPublisher with Logging {

  // Use persistent delivery.
  private val DeliveryMode = MessageProperties.MINIMAL_PERSISTENT_BASIC.getDeliveryMode

  override def publish(event: Event): Future[Unit] = Future {
    logger.debug(s"Publishing event with ID ${event.header.id}")
    channel.basicPublish(exchangeName, routingKey, true, false, properties(event), event.body.content)
    logger.debug(s"Successfully published event with ID ${event.header.id}")
  }

  /** Create RabbitMQ properties from event properties + the required delivery mode properties for reliable delivery. */
  private def properties(event: Event): BasicProperties = {

    // Required properties.
    val builder = new BasicProperties.Builder()
      .deliveryMode(DeliveryMode)
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

