package com.blinkbox.books.rabbitmq

import akka.actor.{ ActorRef, ActorSystem, Props, Status }
import akka.actor.Status.Success
import akka.pattern.ask
import akka.testkit.{ ImplicitSender, TestActorRef, TestEventListener, TestKit }
import akka.util.Timeout
import com.blinkbox.books.messaging.{ ContentType, Event, EventHeader }
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{ Channel, Connection }
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeoutException
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

  val ExchangeName = "test.exchange"
  val Topic = "test.topic"
  val TestMessageTimeout = 10.seconds
  implicit val TestActorTimeout = Timeout(12.seconds)

  //
  // Async actor tests.
  //

  test("Publish XML message to named exchange") {
    val (concurrentActor, channel) = initActor(Some(ExchangeName))

    concurrentActor ! event("test 1")

    within(1000.millis) {
      expectMsgType[Success]
      verify(channel).basicPublish(matcherEq(ExchangeName), matcherEq(Topic), any[BasicProperties], any[Array[Byte]])
    }
  }

  test("Publish message to default exchange") {
    val (concurrentActor, channel) = initActor(None)

    concurrentActor ! event("test 1")

    within(1000.millis) {
      expectMsgType[Success]
      // Should publish on the RabbitMQ "default exchange", whose name is the empty string.
      verify(channel).basicPublish(matcherEq(""), matcherEq(Topic), any[BasicProperties], any[Array[Byte]])
    }
  }

  test("Publish message with content type") {
    val (concurrentActor, channel) = initActor(None)
    concurrentActor ! eventJson("test 1")

    within(1000.millis) {
      expectMsgType[Success]
      val captor = ArgumentCaptor.forClass(classOf[BasicProperties])
      verify(channel).basicPublish(matcherEq(""), matcherEq(Topic), captor.capture(), any[Array[Byte]])
      val props = captor.getValue
      assert(props.getContentType === ContentType.JsonContentType.mediaType)
    }
  }

  test("Message times out") {
    val timeout = 123.millis
    val (concurrentActor, channel) = initActor(None, timeout)

    // Make publishing take some time.
    val timeoutException = new TimeoutException("Test timeout")
    doThrow(timeoutException).when(channel).waitForConfirmsOrDie(anyLong)

    concurrentActor ! event("test")

    // Message should time out.
    val response = expectMsgType[Status.Failure](500.millis)
    assert(response.cause.isInstanceOf[PublishException])
    assert(response.cause.getCause == timeoutException)
    
    // Check that the actor used the right timeout.
    verify(channel).waitForConfirmsOrDie(timeout.toMillis)
  }

  private def event(tag: String): Event = Event.xml("<test/>", EventHeader("test"))
  private def eventJson(tag: String): Event = Event.json("{}", EventHeader("json"))

  private def initActor(exchangeName: Option[String], messageTimeout: FiniteDuration = 1000.millis): (ActorRef, Channel) = {
    val (connection, channel) = mockConnection()

    // Create a waiter so we can wait for the initialisation of the actor, which may happen in another thread.
    val actorInitWaiter = new Waiter()
    doAnswer(() => { actorInitWaiter.dismiss() })
      .when(channel).close()

    // Create actor under test.
    val newActor = system.actorOf(
      Props(new RabbitMqConfirmedPublisher(connection, PublisherConfiguration(exchangeName, Topic, messageTimeout))))

    // Wait for it to be initialised.
    within(1.seconds) {
      actorInitWaiter.await()
    }

    // Reset the mock.
    reset(channel)

    (newActor, channel)
  }

  /** Create a mock RabbitMQ Connection that gives out a mock Channel. */
  private def mockConnection() = {
    val connection = mock[Connection]
    val channel = mock[Channel]
    doReturn(channel).when(connection).createChannel()
    (connection, channel)
  }

}

object RabbitMqConfirmedPublisherTest {

  // Enable actor's logging to be checked.
  val TestEventListener = """
    akka.loggers = ["akka.testkit.TestEventListener"]
    """

}

