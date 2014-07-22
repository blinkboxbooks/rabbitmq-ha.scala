package com.blinkbox.books.rabbitmq

import akka.actor.{ ActorRef, ActorSystem, Props, Status }
import akka.testkit.{ EventFilter, ImplicitSender, TestKit }
import akka.util.Timeout
import com.blinkbox.books.messaging.{ ContentType, Event, EventHeader }
import com.rabbitmq.client.{ Channel, ConfirmListener, Connection }
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Matchers.{ eq => matcherEq }
import org.mockito.Mockito._
import org.scalatest.FunSuiteLike
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{ Seconds, Span }
import scala.concurrent.duration._
import RabbitMqConfirmedPublisherTest._

@RunWith(classOf[JUnitRunner])
class RabbitMqConfirmedPublisherTest extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(Config)))
  with ImplicitSender with FunSuiteLike with MockitoSugar with AsyncAssertions with AnswerSugar {

  import RabbitMqConfirmedPublisher._

  val ExchangeName = "test.exchange"
  val Topic = "test.topic"
  val TestMessageTimeout = 10.seconds
  val TestTimeout = 3.seconds
  implicit val defaultPatience = PatienceConfig(timeout = scaled(Span(5, Seconds)))
  implicit val TestActorTimeout = Timeout(10.seconds)

  test("Publish message to named exchange") {
    val (actor, channel) = initActor(Some(ExchangeName))

    sendEventAndWait(event("test event"), actor)

    // Fake a response from the Channel.
    confirmListener(channel).handleAck(0, false)

    within(TestTimeout) {
      expectMsgType[Status.Success]
      verify(channel).basicPublish(matcherEq(ExchangeName), matcherEq(Topic), any[BasicProperties], any[Array[Byte]])
    }
  }

  test("Publish message to default exchange") {
    val (actor, channel) = initActor(None)

    sendEventAndWait(event("test event"), actor)

    // Fake a response from the Channel.
    confirmListener(channel).handleAck(0, false)

    within(TestTimeout) {
      expectMsgType[Status.Success]
      // Should publish on the RabbitMQ "default exchange", whose name is the empty string.
      verify(channel).basicPublish(matcherEq(""), matcherEq(Topic), any[BasicProperties], any[Array[Byte]])
    }
  }

  test("Publish message with content type") {
    val (actor, channel) = initActor(None)

    sendEventAndWait(eventJson("json test event"), actor)

    // Fake a response from the Channel.
    confirmListener(channel).handleAck(0, false)

    within(TestTimeout) {
      expectMsgType[Status.Success]
      // Should publish on the RabbitMQ "default exchange", whose name is the empty string.
      val captor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel).basicPublish(matcherEq(""), matcherEq(Topic), captor.capture(), any[Array[Byte]])
      assert(captor.getValue.getContentType === ContentType.JsonContentType.mediaType)
    }
  }

  test("Message times out") {
    // Use a real, concurrent actor for this test case, with a very short timeout.
    val (actor, channel) = initActor(None, 100.millis)

    actor ! event("test")

    val response = expectMsgType[Status.Failure](TestTimeout)
    assert(response.cause.isInstanceOf[PublishException])

    // ACKing after timeout should have no effect.
    confirmListener(channel).handleAck(1, false)

    expectNoMsg(1.second)
  }

  private def event(tag: String): Event = Event.xml("<test/>", EventHeader(tag))
  private def eventJson(tag: String): Event = Event.json("{}", EventHeader(tag))

  private def initActor(exchangeName: Option[String], messageTimeout: FiniteDuration = TestMessageTimeout) = {
    val (connection, channel) = mockConnection()

    // Create a waiter so we can wait for the (async) initialisation of the actor.
    val actorInitWaiter = new Waiter()
    doAnswer(() => { actorInitWaiter.dismiss() })
      .when(channel).close()

    // Create actor under test.
    val newActor = system.actorOf(
      Props(new RabbitMqConfirmedPublisher(connection, PublisherConfiguration(exchangeName, Topic, messageTimeout))))

    // Wait for it to be initialised.
    within(TestTimeout) {
      actorInitWaiter.await()
    }

    // Reset the mock channel.
    reset(channel)

    (newActor, channel)
  }

  private def sendEventAndWait(e: Event, sourceActor: ActorRef) {
    EventFilter.debug(pattern = s".*${e.header.id}.*", occurrences = 1, source = sourceActor.path.toString).intercept {
      sourceActor ! e
    }
  }

  /** Create a mock RabbitMQ Connection that gives out a mock Channel. */
  private def mockConnection() = {
    val connection = mock[Connection]
    val channel = mock[Channel]
    doReturn(channel).when(connection).createChannel()
    (connection, channel)
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
  val Config = """
    akka.loggers = ["akka.testkit.TestEventListener"]
    akka.loglevel = DEBUG
    """

}
