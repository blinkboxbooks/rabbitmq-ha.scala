package com.blinkboxbooks.hermes.rabbitmq

import akka.actor.{ ActorSystem, Props, Status }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit }
import akka.util.Timeout
import com.blinkbox.books.messaging._
import com.rabbitmq.client.{ ConfirmListener, MessageProperties, Envelope, Channel, Consumer }
import com.rabbitmq.client.AMQP.BasicProperties
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Date
import org.mockito.{ Matchers, ArgumentCaptor }
import Matchers.{ eq => matcherEq }
import org.mockito.Mockito._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ FunSuiteLike, BeforeAndAfter }
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import RabbitMqConsumer._
import akka.actor.ActorRef
import akka.testkit.TestProbe

@RunWith(classOf[JUnitRunner])
class RabbitMqConsumerTest extends TestKit(ActorSystem("test-system")) with ImplicitSender
  with FunSuiteLike with BeforeAndAfter with MockitoSugar {

  val config = QueueConfiguration("TestQueue", "TestExchange", List("routing.key.1", "routing.key.2"), 10)
  val consumerTag = "consumer-tag"

  val envelope = new Envelope(0, false, config.exchangeName, config.routingKeys(0))
  val originator = "originator"
  val userId = "userId"
  val messageId = "messageId"
  val transactionId = "transactionId"
  val messageContent = "<test>Test message</test>"

  var channel: Channel = _

  before {
    channel = mock[Channel]
  }

  test("Consume message that succeeds, with all optional header fields set") {
    val actor = system.actorOf(Props(new RabbitMqConsumer(channel, config, consumerTag, self)))
    waitUntilStarted(actor)

    val customHeaders = Map[String, Object](RabbitMqConsumer.TransactionIdHeader -> transactionId).asJava

    // Check consumer registered and get hold of the registered callback.
    val consumerArgument = ArgumentCaptor.forClass(classOf[Consumer])
    verify(channel).basicConsume(matcherEq(config.queueName), matcherEq(false), matcherEq(consumerTag), consumerArgument.capture)

    // Send a message through the callback and check that it gets handled correctly.
    val messageTimestamp = new Date()
    val properties = new BasicProperties.Builder()
      .messageId(messageId)
      .timestamp(messageTimestamp)
      .appId(originator)
      .userId(userId)
      .contentEncoding(StandardCharsets.UTF_8.name)
      .contentType(ContentType.XmlContentType.mediaType)
      .headers(customHeaders)
      .build()

    // Send a message through the callback.
    consumerArgument.getValue.handleDelivery(consumerTag, envelope, properties, messageContent.getBytes(StandardCharsets.UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]

      assert(event.header.id == messageId
        && event.body.contentType.charset.isDefined
        && event.body.contentType.charset.get == StandardCharsets.UTF_8
        && event.body.asString == messageContent
        && event.header.originator == originator
        && event.header.userId == Some(userId)
        && event.header.transactionId == Some(transactionId)
        && event.header.timestamp.getMillis == messageTimestamp.getTime)
    }
  }

  test("Consume message without optional header fields") {
    fail("TODO")
  }

  test("Produce message that succeeds") {
    fail("TODO")
  }

  test("Create queue configuration from standard config") {
    fail("TODO")
  }

  // Ensure actor has started by passing a message through it.
  private def waitUntilStarted(actor: ActorRef) {
    val probeMsg = RabbitMqMessage(-1, envelope, new BasicProperties(), "Probe message".getBytes)
    actor ! probeMsg
    receiveOne(900.millis)
  }

}
