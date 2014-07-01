package com.blinkbox.books.rabbitmq

import akka.actor.{ ActorRef, ActorSystem, Props, Status }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit, TestProbe }
import akka.util.Timeout
import com.blinkbox.books.messaging._
import com.rabbitmq.client.{ ConfirmListener, MessageProperties, Envelope, Channel, Consumer }
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.config.{ Config, ConfigFactory }
import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
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
import RabbitMqConsumerTest._
import akka.testkit.EventFilter
import scala.reflect.ClassTag
import java.nio.charset.UnsupportedCharsetException

object RabbitMqConsumerTest {
  // Enable actor's logging to be checked.
  val TestEventListener = """
    akka.loggers = ["akka.testkit.TestEventListener"]
    """
}

@RunWith(classOf[JUnitRunner])
class RabbitMqConsumerTest extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(TestEventListener)))
  with ImplicitSender with FunSuiteLike with BeforeAndAfter with MockitoSugar with AsyncAssertions with AnswerSugar {

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

  test("Consume message that succeeds, with all optional header fields set") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor()

    // Add optional properties.
    val customHeaders = Map[String, Object](RabbitMqConsumer.TransactionIdHeader -> transactionId).asJava
    val properties = basicProperties
      .userId(userId)
      .headers(customHeaders)
      .build()

    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope, properties, messageContent.getBytes(UTF_8))

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
      verify(channel).basicAck(envelope.getDeliveryTag, false)
      verify(channel, never).basicReject(anyLong, anyBoolean)
    }

  }

  test("Consume message without optional header fields") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor()

    // Send a test message through the callback.
    consumer.handleDelivery(consumerTag, envelope, basicProperties.build(), messageContent.getBytes(UTF_8))

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
    verify(channel).basicAck(envelope.getDeliveryTag, false)
    verify(channel, never).basicReject(anyLong, anyBoolean)
  }

  test("Incoming message without required fields") {
    checkRejectsInvalidMessage[IllegalArgumentException](new BasicProperties.Builder().build)
  }

  test("Incoming message with invalid charset") {
    checkRejectsInvalidMessage[UnsupportedCharsetException](basicProperties.contentEncoding("INVALID").build)
  }

  def checkRejectsInvalidMessage[T <: Throwable: ClassTag](properties: BasicProperties) = {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor()

    within(1000.millis) {
      // Invalid message should be logged, rejected, and not passed on..
      EventFilter[T](pattern = ".*invalid.*", source = actor.path.toString, occurrences = 1) intercept {
        // Trigger input message.
        consumer.handleDelivery(consumerTag, envelope, properties, messageContent.getBytes(UTF_8))
        expectNoMsg
      }
      rejectWaiter.await()
      verify(channel).basicReject(envelope.getDeliveryTag, false)
      verify(channel, never).basicAck(anyLong, anyBoolean)
    }
  }

  test("Message that fails to process") {
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor()

    // Trigger input message.
    consumer.handleDelivery(consumerTag, envelope, basicProperties.build(), messageContent.getBytes(UTF_8))

    within(1.seconds) {
      val message = receiveOne(500.millis)
      val event = message.asInstanceOf[Event]
    }

    // Respond with Failure.
    lastSender ! Status.Failure(new Exception("Test Exception"))

    // Check that message was rejected and re-queued.
    rejectWaiter.await()
    verify(channel).basicReject(envelope.getDeliveryTag, false)
    verify(channel, never).basicAck(anyLong, anyBoolean)
  }

  test("Consumer with no topics") {
    val configuration = config.copy(routingKeys = Seq())
    val (channel, actor, consumer, ackWaiter, rejectWaiter) = setupActor(configuration)

    // Shouldn't declare a topic exchange if no routing key is given,
    // to cope with old-style services that use manually created fanout exchanges.
    verify(channel, times(0)).exchangeDeclare(anyString, anyString)
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
    .contentEncoding(UTF_8.name)
    .contentType(ContentType.XmlContentType.mediaType)

  private def waitUntilStarted(actor: ActorRef) {
    actor ! Init
    within(900.millis) {
      expectMsgType[Status.Success]
    }
  }

  private def setupActor(configuration: QueueConfiguration = config): (Channel, ActorRef, Consumer, Waiter, Waiter) = {
    val channel = mock[Channel]

    // Create actor under test.
    val actor = system.actorOf(Props(new RabbitMqConsumer(channel, config, consumerTag, self)))
    waitUntilStarted(actor)

    // Check consumer registered and get hold of the registered callback.
    val consumerArgument = ArgumentCaptor.forClass(classOf[Consumer])
    verify(channel).basicConsume(matcherEq(config.queueName), matcherEq(false), matcherEq(consumerTag), consumerArgument.capture)
    val consumer = consumerArgument.getValue

    // Set up waiters for acks/rejects.
    val ackWaiter = new Waiter()
    val rejectWaiter = new Waiter()

    doAnswer(() => { ackWaiter.dismiss() }).when(channel).basicAck(anyLong, anyBoolean)
    doAnswer(() => { rejectWaiter.dismiss() }).when(channel).basicReject(anyLong, anyBoolean)
    (channel, actor, consumer, ackWaiter, rejectWaiter)
  }

}
