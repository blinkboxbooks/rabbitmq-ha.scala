package com.blinkbox.books.rabbitmq

import akka.actor.{ ActorRef, ActorSystem, Props, Status }
import akka.actor.Status.Success
import akka.pattern.ask
import akka.testkit.{ ImplicitSender, TestActorRef, TestEventListener, TestKit }
import akka.util.Timeout
import com.blinkbox.books.messaging.{ContentType, Event, EventHeader}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{ Channel, ConfirmListener }
import com.typesafe.config.ConfigFactory
import java.util.concurrent.atomic.AtomicLong
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Matchers.{ eq => matcherEq }
import org.mockito.Mockito._
import org.scalatest.{ BeforeAndAfterEach, FunSuiteLike }
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._

import RabbitMqConfirmedPublisherTest._

@RunWith(classOf[JUnitRunner])
class RabbitMqConfirmedPublisherTest extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(TestEventListener)))
  with ImplicitSender with FunSuiteLike with MockitoSugar with AsyncAssertions with AnswerSugar {

  import RabbitMqConfirmedPublisher._

  val TopicExchangeName = "test.exchange"
  val HeadersExchangeName = "test.headers.exchange"
  val Topic = "test.topic"
  var `type` = "topic"
  val TestMessageTimeout = 10.seconds
  var args = None
  implicit val TestActorTimeout = Timeout(10.seconds)

  //
  // Synchronous tests. These check the confirmation handling in the main actor,
  // but not publishing (as this is handled by a created child actor, which
  // won't behave synchronously).
  //

  test("Single acked message") {
    // Initialise actor and related mocks.
    val (actor, channel, confirmListener) = setupActor(Some(TopicExchangeName))

    // Ask actor to publish message.
    val response = actor ? event("test 1")

    // Fake a response from the Channel.
    confirmListener.handleAck(0, false)

    val util.Success(_) = response.value.get
    assert(actor.underlyingActor.pendingMessages.isEmpty)
  }

  test("Single acked message to default exchange") {
    val (actor, channel, confirmListener) = setupActor(None)

    val response = actor ? event("test 1")

    confirmListener.handleAck(0, false)

    val util.Success(_) = response.value.get
    assert(actor.underlyingActor.pendingMessages.isEmpty)
  }

  test("Single nacked message") {
    val (actor, channel, confirmListener) = setupActor(Some(TopicExchangeName))
    val response = actor ? event("test 1")
    confirmListener.handleNack(0, false)

    val util.Failure(PublishException(message, _)) = response.value.get
    assert(message.contains("not successfully received"))

    assert(actor.underlyingActor.pendingMessages.isEmpty)
  }

  test("Single acked message while others remain") {
    val (actor, channel, confirmListener) = setupActor(Some(TopicExchangeName))
    actor ! event("test 1")
    val response = actor ? event("test 2")
    actor ! event("test 3")

    // Only ACK the middle message.
    confirmListener.handleAck(1, false)

    val util.Success(_) = response.value.get

    // Should leave other messages pending.
    assert(actor.underlyingActor.pendingMessages.keySet == Set(0, 2))
  }

  test("Single nacked message while others remain") {
    val (actor, channel, confirmListener) = setupActor(Some(TopicExchangeName))
    actor ! event("test 1")
    val response = actor ? event("test 2")
    actor ! event("test 3")

    // Only NACK the middle message.
    confirmListener.handleNack(1, false)

    val util.Failure(PublishException(reason, _)) = response.value.get

    // Should leave other messages pending.
    assert(actor.underlyingActor.pendingMessages.keySet == Set(0, 2))
  }

  test("Ack multiple messages") {
    val (actor, channel, confirmListener) = setupActor(Some(TopicExchangeName))
    val response1 = actor ? event("test 1")
    val response2 = actor ? event("test 2")
    actor ! event("test 3")

    // ACK messages up to and including the second one.
    confirmListener.handleAck(1, true)

    val util.Success(_) = response1.value.get
    val util.Success(_) = response2.value.get

    // Should leave later message pending.
    assert(actor.underlyingActor.pendingMessages.keySet == Set(2))
  }

  test("Ack for unknown message") {
    val (actor, channel, confirmListener) = setupActor(Some(TopicExchangeName))
    actor ! event("test 1")
    actor ! event("test 2")
    actor ! event("test 3")

    // ACK messages up to and including the second one.
    confirmListener.handleAck(42, false)

    // Should leave all message pending.
    assert(actor.underlyingActor.pendingMessages.keySet == Set(0, 1, 2))
  }

  //
  // Async tests. These check publishing, i.e. the actions handled in created child actors.
  //

  test("Publish message to named topic exchange") {
    val (concurrentActor, channel, confirmListener) = asyncActor(Some(TopicExchangeName), Some(Topic))

    concurrentActor ! event("test 1")

    // Fake a response from the Channel.
    confirmListener.handleAck(0, false)

    within(1000.millis) {
      expectMsgType[Success]
      verify(channel).basicPublish(matcherEq(TopicExchangeName), matcherEq(Topic), any[BasicProperties], any[Array[Byte]])
    }
  }
  
  test("Publish message to named headers exchange") {
    val (concurrentActor, channel, confirmListener) = asyncActor(Some(HeadersExchangeName), None)
    `type` = "headers"

    concurrentActor ! event("test 1")

    // Fake a response from the Channel.
    confirmListener.handleAck(0, false)

    within(1000.millis) {
      expectMsgType[Success]
      verify(channel).basicPublish(matcherEq(HeadersExchangeName), matcherEq(""), any[BasicProperties], any[Array[Byte]])
    }
  }

  test("Publish message to default exchange") {
    val (concurrentActor, channel, confirmListener) = asyncActor(None, Some(Topic))

    concurrentActor ! event("test 1")

    // Fake a response from the Channel.
    confirmListener.handleAck(0, false)

    within(1000.millis) {
      expectMsgType[Success]
      // Should publish on the RabbitMQ "default exchange", whose name is the empty string.
      verify(channel).basicPublish(matcherEq(""), matcherEq(Topic), any[BasicProperties], any[Array[Byte]])
    }
  }

  test("Publish message with content type") {
    val (concurrentActor, channel, confirmListener) = asyncActor(None, Some(Topic))
    concurrentActor ! eventJson("test 1")

    // Fake a response from the Channel.
    confirmListener.handleAck(0, false)

    within(1000.millis) {
      expectMsgType[Success]
      // Should publish on the RabbitMQ "default exchange", whose name is the empty string.
      val captor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel).basicPublish(matcherEq(""), matcherEq(Topic), captor.capture() , any[Array[Byte]])
      val props  = captor.getValue
      assert(props.getContentType === ContentType.JsonContentType.mediaType)
    }
  }

  test("Message times out") {
    // Use a real, concurrent actor for this test case, with a very short timeout.
    val (concurrentActor, channel, confirmListener) = asyncActor(None, Some(Topic), 100.millis)

    concurrentActor ! event("test")

    val response = expectMsgType[Status.Failure](1.second)
    assert(response.cause.isInstanceOf[PublishException])

    // ACKing after timeout should have no effect.
    confirmListener.handleAck(1, false)

    expectNoMsg(1.second)
  }

  private def event(tag: String): Event = Event.xml("<test/>", EventHeader("test"))
  private def eventJson(tag: String): Event = Event.json("{}", EventHeader("json"))

  private def setupActor(exchangeName: Option[String]): (TestActorRef[RabbitMqConfirmedPublisher], Channel, ConfirmListener) = {
    val channel = mockChannel()
    val newActor = TestActorRef(new RabbitMqConfirmedPublisher(channel, PublisherConfiguration(exchangeName, Some(Topic), args, TestMessageTimeout, `type`)))
    (newActor, channel, confirmListener(channel))
  }

  private def asyncActor(exchangeName: Option[String], routingKey : Option[String], messageTimeout: FiniteDuration = 1000.millis): (ActorRef, Channel, ConfirmListener) = {
    val channel = mockChannel()

    // Create a waiter so we can wait for the (async) initialisation of the actor.
    val actorInitWaiter = new Waiter()
    doAnswer(() => { actorInitWaiter.dismiss() })
      .when(channel).addConfirmListener(any[ConfirmListener])

    // Create actor under test.
    val newActor = system.actorOf(
      Props(new RabbitMqConfirmedPublisher(channel, PublisherConfiguration(exchangeName, routingKey, args, messageTimeout, `type`))))

    // Wait for it to be initialised.
    within(1.seconds) {
      actorInitWaiter.await()
    }

    (newActor, channel, confirmListener(channel))
  }

  private def asyncActorForHeaders(exchangeName: Option[String], messageTimeout: FiniteDuration = 1000.millis): (ActorRef, Channel, ConfirmListener) = {
    val channel = mockChannel()

    // Create a waiter so we can wait for the (async) initialisation of the actor.
    val actorInitWaiter = new Waiter()
    doAnswer(() => { actorInitWaiter.dismiss() })
      .when(channel).addConfirmListener(any[ConfirmListener])

    // Create actor under test.
    val newActor = system.actorOf(
      Props(new RabbitMqConfirmedPublisher(channel, PublisherConfiguration(exchangeName, None, args,  messageTimeout, `type`))))

    // Wait for it to be initialised.
    within(1.seconds) {
      actorInitWaiter.await()
    }

    (newActor, channel, confirmListener(channel))
  }

  /** Create a mock RabbitMQ Channel that gives out valid sequence numbers.  */
  private def mockChannel() = {
    val channel = mock[Channel]
    val seqNo = new AtomicLong(0L)
    doAnswer(() => { seqNo.getAndIncrement }).when(channel).getNextPublishSeqNo
    channel
  }

  /** Get hold of the actor's registered ConfirmListener. */
  private def confirmListener(channel: Channel) = {
    val confirmListenerArgument = ArgumentCaptor.forClass(classOf[ConfirmListener])
    verify(channel).addConfirmListener(confirmListenerArgument.capture())
    val confirmListener = confirmListenerArgument.getValue
    assert(confirmListenerArgument.getValue != null, "Actor should have registered a confirm listener")
    confirmListener
  }
}

object RabbitMqConfirmedPublisherTest {

  // Enable actor's logging to be checked.
  val TestEventListener = """
    akka.loggers = ["akka.testkit.TestEventListener"]
    """

}

