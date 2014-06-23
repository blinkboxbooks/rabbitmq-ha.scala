package com.blinkboxbooks.hermes.rabbitmq

import akka.actor.{ ActorRef, ActorSystem, Props, Status }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit, TestProbe }
import akka.util.Timeout
import com.blinkbox.books.messaging._
import com.rabbitmq.client.{ ConfirmListener, MessageProperties, Envelope, Channel, Consumer }
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.config.{ Config, ConfigFactory }
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Date
import org.junit.runner.RunWith
import org.mockito.{ Matchers, ArgumentCaptor }
import org.mockito.Matchers.{ eq => matcherEq }
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ FunSuiteLike, BeforeAndAfter }
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RabbitMqConsumerTest extends TestKit(ActorSystem("test-system")) with ImplicitSender
  with FunSuiteLike with BeforeAndAfter with MockitoSugar with AsyncAssertions with AnswerSugar {

  import RabbitMqConsumer._

  val config = QueueConfiguration("TestQueue", "TestExchange", List("routing.key.1", "routing.key.2"), 10)
  val consumerTag = "consumer-tag"

  val envelope = new Envelope(0, false, config.exchangeName, config.routingKeys(0))
  val originator = "originator"
  val userId = "userId"
  val messageId = "messageId"
  val transactionId = "transactionId"
  val messageContent = "<test>Test message</test>"
  val messageTimestamp = new Date()

  val ackWaiter = new Waiter
  val nackWaiter = new Waiter

  var channel: Channel = _
  var actor: ActorRef = _
  var consumer: Consumer = _

  before {
    channel = mock[Channel]

    // Create actor under test.
    actor = system.actorOf(Props(new RabbitMqConsumer(channel, config, consumerTag, self)))
    waitUntilStarted(actor)

    // Check consumer registered and get hold of the registered callback.
    val consumerArgument = ArgumentCaptor.forClass(classOf[Consumer])
    verify(channel).basicConsume(matcherEq(config.queueName), matcherEq(false), matcherEq(consumerTag), consumerArgument.capture)
    consumer = consumerArgument.getValue

    // Set up waiters for acks/nacks.
    doAnswer(() => { ackWaiter.dismiss() }).when(channel).basicAck(anyLong, anyBoolean)
    doAnswer(() => { nackWaiter.dismiss() }).when(channel).basicNack(anyLong, anyBoolean, anyBoolean)
  }

  test("Consume message that succeeds, with all optional header fields set") {
    // Add optional properties.
    val customHeaders = Map[String, Object](RabbitMqConsumer.TransactionIdHeader -> transactionId).asJava
    val properties = basicProperties
      .userId(userId)
      .headers(customHeaders)
      .build()

    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope, properties, messageContent.getBytes(StandardCharsets.UTF_8))

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

      // Respond with success.
      lastSender ! Status.Success("OK")

      // Check that message was acked.
      ackWaiter.await()
      verify(channel).basicAck(envelope.getDeliveryTag, false)
      verify(channel, never).basicNack(anyLong, anyBoolean, anyBoolean)
    }

  }

  test("Consume message without optional header fields") {
    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope, basicProperties.build(), messageContent.getBytes(StandardCharsets.UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]

      assert(event.header.id == messageId
        && event.body.contentType.charset.isDefined
        && event.body.contentType.charset.get == StandardCharsets.UTF_8
        && event.body.asString == messageContent
        && event.header.originator == originator
        && event.header.timestamp.getMillis == messageTimestamp.getTime)
    }

    // Respond with success.
    lastSender ! Status.Success("OK")

    // Check that message was acked.
    ackWaiter.await()
    verify(channel).basicAck(envelope.getDeliveryTag, false)
    verify(channel, never).basicNack(anyLong, anyBoolean, anyBoolean)
  }

  test("Message that fails to process") {

    // Send a message through the callback.
    consumer.handleDelivery(consumerTag, envelope, basicProperties.build(), messageContent.getBytes(StandardCharsets.UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]
    }

    // Respond with Failure.
    lastSender ! Status.Failure(new Exception("Test Exception"))

    // Check that message was nacked and re-queued.
    nackWaiter.await()
    verify(channel).basicNack(envelope.getDeliveryTag, false, true)
    verify(channel, never).basicAck(anyLong, anyBoolean)
  }

  test("Create queue configuration from standard config") {
    val config = ConfigFactory.load("rabbitmq-consumer-test").getConfig("service.test.testQueue")
    val queueConfig = QueueConfiguration(config)
    assert(queueConfig.queueName == "test queue name" &&
      queueConfig.exchangeName == "test exchange name" &&
      queueConfig.routingKeys == List("test key 1", "test key 2", "test key 3") &&
      queueConfig.prefetchCount == 42)
  }

  private def basicProperties = new BasicProperties.Builder()
    .messageId(messageId)
    .timestamp(messageTimestamp)
    .appId(originator)
    .contentEncoding(StandardCharsets.UTF_8.name)
    .contentType(ContentType.XmlContentType.mediaType)

  private def waitUntilStarted(actor: ActorRef) {
    actor ! Init
    within(900.millis) {
      expectMsgType[Status.Success]
    }
  }

}
