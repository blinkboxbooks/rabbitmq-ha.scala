package com.blinkbox.books.rabbitmq

import akka.actor.{ ActorRef, ActorSystem, Props, Status }
import akka.testkit.{ EventFilter, ImplicitSender, TestActorRef, TestKit, TestProbe }
import akka.util.Timeout
import com.blinkbox.books.messaging._
import com.blinkbox.books.test.MockitoSyrup
import com.rabbitmq.client.{ ConfirmListener, MessageProperties, Envelope, Channel, Consumer }
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.config.{ Config, ConfigFactory }
import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.charset.UnsupportedCharsetException
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
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import RabbitMqConsumerTest._
import scala.reflect.ClassTag

object RabbitMqConsumerTest {
  // Enable actor's logging to be checked.
  val TestEventListener = """
    akka.loggers.0 = "akka.testkit.TestEventListener"
    """
}

@RunWith(classOf[JUnitRunner])
class RabbitMqConsumerTest extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(TestEventListener)))
  with ImplicitSender with FunSuiteLike with BeforeAndAfter with MockitoSyrup with AsyncAssertions {

  import RabbitMqConsumer._

  val consumerTag = "consumer-tag"
  val originator = "originator"
  val userId = "userId"
  val messageId = "messageId"
  val transactionId = "transactionId"
  val messageContent = "<test>Test message</test>"
  val messageTimestamp = new Date()

  private def topicExchangeConfig = QueueConfiguration("TestQueue", "TestExchange", "topic", List("routing.key.1", "routing.key.2"), Map[String, AnyRef](), 10)
  private def headerExchangeConfig = QueueConfiguration("TestQueue", "TestExchange", "headers", List(), Map[String, AnyRef]("content-type" -> "test-content-type"), 10)
  private def fanoutExchangeConfig = QueueConfiguration("TestQueue", "TestExchange", "fanout", List(), Map[String, AnyRef](), 10)

  private def envelope(config: QueueConfiguration) = 
    new Envelope(0, false, config.exchangeName, if (config.routingKeys.isEmpty) "" else config.routingKeys(0))

  test("Consume message that succeeds, with all optional header fields set") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(topicExchangeConfig)

    // Add optional properties.
    val customHeaders = Map[String, Object](
      RabbitMqConsumer.TransactionIdHeader -> transactionId,
      RabbitMqConsumer.UserIdHeader -> userId).asJava
    val properties = basicProperties
      .headers(customHeaders)
      .build()

    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope(topicExchangeConfig), properties, messageContent.getBytes(UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]

      assert(event.header.id == messageId
        && event.body.contentType.charset.isDefined
        && event.body.contentType.charset.get == UTF_8
        && event.body.asString == messageContent
        && event.header.originator == originator
        && event.header.userId == Some(userId)
        && event.header.transactionId == Some(transactionId)
        && event.header.timestamp.getMillis == messageTimestamp.getTime)

      // Respond with success.
      lastSender ! Status.Success("OK")

      // Check that message was acked.
      ackWaiter.await()
      verify(channel).basicAck(envelope(topicExchangeConfig).getDeliveryTag, false)
      verify(channel, never).basicReject(anyLong, anyBoolean)
    }

  }

  test("Consume message without optional header fields") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(headerExchangeConfig)

    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope(headerExchangeConfig), basicProperties.build(), messageContent.getBytes(UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]

      assert(event.header.id == messageId
        && event.body.contentType.charset.isDefined
        && event.body.contentType.charset.get == UTF_8
        && event.body.asString == messageContent
        && event.header.originator == originator
        && event.header.timestamp.getMillis == messageTimestamp.getTime)
    }

    // Respond with success.
    lastSender ! Status.Success("OK")

    // Check that message was acked.
    ackWaiter.await()
    verify(channel).basicAck(envelope(headerExchangeConfig).getDeliveryTag, false)
    verify(channel, never).basicReject(anyLong, anyBoolean)
  }
  
  test("Consumer fails to initialise") {
    val channel = mock[Channel]
    val ex = new RuntimeException("Test exception in initialistion")
    doThrow(ex).when(channel).queueDeclare(anyString, anyBoolean, anyBoolean, anyBoolean, matcherEq(null))

    // Create actor under test.
    val actor = system.actorOf(Props(new RabbitMqConsumer(channel, headerExchangeConfig, consumerTag, self)))

    actor ! Init
    within(900.millis) {
      // Should respond with failure to client.
      expectMsgType[Status.Failure]
    }
    
  }
  
  test("Consume JSON message") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(headerExchangeConfig)

    val mediaType = "application/vnd.blinkbox.books.events.test.event.v2+json"
    val properties = basicProperties.contentType(mediaType).build()
    
    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope(headerExchangeConfig), properties, messageContent.getBytes(UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]

      assert(event.header.id == messageId
        && event.body.contentType.charset.isDefined
        && event.body.contentType.charset.get == UTF_8
        && event.body.contentType.mediaType.toString == mediaType
        && event.body.asString == messageContent
        && event.header.originator == originator
        && event.header.timestamp.getMillis == messageTimestamp.getTime)
    }

    // Respond with success.
    lastSender ! Status.Success("OK")

    // Check that message was acked.
    ackWaiter.await()
    verify(channel).basicAck(envelope(headerExchangeConfig).getDeliveryTag, false)
    verify(channel, never).basicReject(anyLong, anyBoolean)
  }

  // Falls back to defaults at the moment, to cope with legacy messages.
  test("Incoming message without required fields") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(headerExchangeConfig)

    // Handle a message with no header fields.
    val properties = new BasicProperties.Builder().build

    consumer.handleDelivery(consumerTag, envelope(headerExchangeConfig), properties, messageContent.getBytes(UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]

      assert(event.header.id.length == 36 // Should be a GUID.
        && event.body.contentType == ContentType.XmlContentType
        && event.body.asString == messageContent
        && event.header.originator == "unknown"
        && event.header.userId == None
        && event.header.transactionId == None)
    }
  }

  test("Incoming message with invalid charset") {
    checkRejectsInvalidMessage[UnsupportedCharsetException](basicProperties.contentEncoding("INVALID").build)
  }

  def checkRejectsInvalidMessage[T <: Throwable: ClassTag](properties: BasicProperties) = {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(headerExchangeConfig)

    within(1000.millis) {
      // Invalid message should be logged, rejected, and not passed on..
      EventFilter[T](pattern = ".*invalid.*", source = actor.path.toString, occurrences = 1) intercept {
        // Trigger input message.
        consumer.handleDelivery(consumerTag, envelope(headerExchangeConfig), properties, messageContent.getBytes(UTF_8))
        expectNoMsg
      }
      rejectWaiter.await()
      verify(channel).basicReject(envelope(headerExchangeConfig).getDeliveryTag, false)
      verify(channel, never).basicAck(anyLong, anyBoolean)
    }
  }

  test("Message that fails to process") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(headerExchangeConfig)

    // Trigger input message.
    consumer.handleDelivery(consumerTag, envelope(headerExchangeConfig), basicProperties.build(), messageContent.getBytes(UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]
    }

    // Respond with Failure.
    lastSender ! Status.Failure(new Exception("Test Exception"))

    // Check that message was rejected and re-queued.
    rejectWaiter.await()
    verify(channel).basicReject(envelope(headerExchangeConfig).getDeliveryTag, false)
    verify(channel, never).basicAck(anyLong, anyBoolean)
  }

  test("Consumer with header exchange") {
    val (channel, _, _, _, _) = setupActor(headerExchangeConfig)
    verify(channel).exchangeDeclare(anyString, matcherEq("headers"), matcherEq(true))
  }

  test("Consumer with topic exchange") {
    val (channel, _, _, _, _) = setupActor(topicExchangeConfig)
    verify(channel).exchangeDeclare(anyString, matcherEq("topic"), matcherEq(true))
  }

  test("Consumer with fanout exchange") {
    val (channel, _, _, _, _) = setupActor(fanoutExchangeConfig)
    verify(channel).exchangeDeclare(anyString, matcherEq("fanout"), matcherEq(true))
  }

  test("Create queue configuration with header exchange from config") {
    val config = ConfigFactory.load("rabbitmq-consumer-header-exchange").getConfig("service.test.testQueue")
    val queueConfig = QueueConfiguration(config)
    assert(queueConfig.queueName == "test queue name" &&
      queueConfig.exchangeName == "test exchange name" &&
      queueConfig.routingKeys == List() &&
      queueConfig.headerArgs == Map("foo" -> "1", "bar" -> 2) &&
      queueConfig.prefetchCount == 1000)
  }

  test("Create queue configuration with topic exchange from config") {
    val config = ConfigFactory.load("rabbitmq-consumer-topic-exchange").getConfig("service.test.testQueue")
    val queueConfig = QueueConfiguration(config)
    assert(queueConfig.queueName == "test queue name" &&
      queueConfig.exchangeName == "test exchange name" &&
      queueConfig.routingKeys == List("test key 1", "test key 2", "test key 3") &&
      queueConfig.prefetchCount == 42)
  }

  test("Create queue configuration with fanout exchange from config") {
    val config = ConfigFactory.load("rabbitmq-consumer-fanout-exchange").getConfig("service.test.testQueue")
    val queueConfig = QueueConfiguration(config)
    assert(queueConfig.queueName == "test queue name" &&
      queueConfig.exchangeName == "test exchange name" &&
      queueConfig.routingKeys == List() &&
      queueConfig.prefetchCount == 123)
  }

  test("Header exchange config without header arguments") {
    intercept[IllegalArgumentException] { headerExchangeConfig.copy(headerArgs = Map()) }
  }

  test("Topic exchange config without routing keys") {
    intercept[IllegalArgumentException] { topicExchangeConfig.copy(routingKeys = List()) }
  }

  test("Specifying both routing keys and header arguments") {
    intercept[IllegalArgumentException] { headerExchangeConfig.copy(routingKeys = List("routing-key")) }
  }

  private def basicProperties = new BasicProperties.Builder()
    .messageId(messageId)
    .timestamp(messageTimestamp)
    .appId(originator)
    .contentEncoding(UTF_8.name)
    .contentType(ContentType.XmlContentType.mediaType.toString())

  private def waitUntilStarted(actor: ActorRef) {
    actor ! Init
    within(900.millis) {
      expectMsgType[Status.Success]
    }
  }

  private def setupActor(configuration: QueueConfiguration): (Channel, ActorRef, Consumer, Waiter, Waiter) = {
    val channel = mock[Channel]

    // Create actor under test.
    val actor = system.actorOf(Props(new RabbitMqConsumer(channel, configuration, consumerTag, self)))
    waitUntilStarted(actor)

    // Check consumer registered and get hold of the registered callback.
    val consumerArgument = ArgumentCaptor.forClass(classOf[Consumer])
    verify(channel).basicConsume(matcherEq(configuration.queueName), matcherEq(false), matcherEq(consumerTag), consumerArgument.capture)
    val consumer = consumerArgument.getValue

    // Set up waiters for acks/rejects.
    val ackWaiter = new Waiter()
    val rejectWaiter = new Waiter()

    doAnswer(() => { ackWaiter.dismiss() }).when(channel).basicAck(anyLong, anyBoolean)
    doAnswer(() => { rejectWaiter.dismiss() }).when(channel).basicReject(anyLong, anyBoolean)
    (channel, actor, consumer, ackWaiter, rejectWaiter)
  }
}
