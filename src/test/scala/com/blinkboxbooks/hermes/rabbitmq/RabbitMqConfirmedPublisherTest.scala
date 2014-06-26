package com.blinkboxbooks.hermes.rabbitmq

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Status
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestEventListener
import akka.testkit.TestKit
import akka.util.Timeout
import com.blinkbox.books.messaging.{ Event, EventHeader }
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConfirmListener
import com.typesafe.config.ConfigFactory
import java.util.concurrent.atomic.AtomicLong
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuiteLike
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._
import RabbitMqConfirmedPublisherTest._

@RunWith(classOf[JUnitRunner])
class RabbitMqConfirmedPublisherTest extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(TestEventListener)))
  with ImplicitSender with FunSuiteLike with BeforeAndAfterEach with MockitoSugar with AsyncAssertions with AnswerSugar {

  import RabbitMqConfirmedPublisher._

  val ExchangeName = "test.exchange"
  val Topic = "test.topic"
  implicit val TestTimeout = Timeout(10.seconds)
  val TestMessageTimeout = 10.seconds

  var channel: Channel = _
  var actor: TestActorRef[RabbitMqConfirmedPublisher] = _
  var confirmListener: ConfirmListener = _
  var seqNo: AtomicLong = _

  override def beforeEach() {
    seqNo = new AtomicLong(0L)

    // Create a mock RabbitMQ Channel that gives out valid sequence numbers.
    channel = mock[Channel]
    doAnswer(() => { seqNo.getAndIncrement }).when(channel).getNextPublishSeqNo

    // Create actor under test.
    actor = TestActorRef(new RabbitMqConfirmedPublisher(channel, ExchangeName, Topic, TestMessageTimeout))

    // Get hold of the actor's registered ConfirmListener.
    val confirmListenerArgument = ArgumentCaptor.forClass(classOf[ConfirmListener])
    verify(channel).addConfirmListener(confirmListenerArgument.capture())
    confirmListener = confirmListenerArgument.getValue
    assert(confirmListenerArgument.getValue != null, "Actor should have registered a confirm listener")
  }

  test("Single acked message") {
    // Ask actor to publish message.
    val response = actor ? PublishRequest(event("test 1"))

    // Fake a repsonse from the Channel.
    confirmListener.handleAck(0, false)

    val util.Success(_) = response.value.get
    assert(actor.underlyingActor.pendingConfirmation.isEmpty)
  }

  test("Single nacked message") {
    val response = actor ? PublishRequest(event("test 1"))
    confirmListener.handleNack(0, false)

    val util.Failure(PublishException(message, _)) = response.value.get
    assert(message.contains("not successfully received"))

    assert(actor.underlyingActor.pendingConfirmation.isEmpty)
  }

  test("Single acked message while others remain") {
    actor ! PublishRequest(event("test 1"))
    val response = actor ? PublishRequest(event("test 2"))
    actor ! PublishRequest(event("test 3"))

    // Only ACK the middle message.
    confirmListener.handleAck(1, false)

    val util.Success(_) = response.value.get

    // Should leave other messages pending.
    assert(actor.underlyingActor.pendingConfirmation.keySet == Set(0, 2))
  }

  test("Single nacked message while others remain") {
    actor ! PublishRequest(event("test 1"))
    val response = actor ? PublishRequest(event("test 2"))
    actor ! PublishRequest(event("test 3"))

    // Only ACK the middle message.
    confirmListener.handleNack(1, false)

    val util.Failure(PublishException(reason, _)) = response.value.get

    // Should leave other messages pending.
    assert(actor.underlyingActor.pendingConfirmation.keySet == Set(0, 2))
  }

  test("Acked multiple messages") {
    fail("TODO")
  }

  test("Nacked multiple messages") {
    fail("TODO")
  }

  test("Acked multiple messages while others remain") {
    fail("TODO")
  }

  test("Nacked multiple messages remain") {
    fail("TODO")
  }

  test("Receiving ack for unknown message") {
    fail("TODO")
  }

  test("Receiving nack for unknown message") {
    // ## Simplify the ack+nack test cases?
    fail("TODO")
  }

  test("Message times out") {
    // Use a real, concurrent actor for this test case, with a very short timeout.
    val concurrentActor = system.actorOf(Props(new RabbitMqConfirmedPublisher(channel, ExchangeName, Topic, 100.millis)))

    concurrentActor ! PublishRequest(event("test"))

    val response = expectMsgType[Status.Failure](1.second)
    assert(response.cause.isInstanceOf[PublishException])

    // ACKing after timeout should have no effect.
    confirmListener.handleAck(1, false)

    expectNoMsg(1.second)
  }

  private def event(tag: String): Event = Event.xml("<test/>", EventHeader("test"))

}

object RabbitMqConfirmedPublisherTest {
  // Enable actor's logging to be checked.
  val TestEventListener = """
    akka.loggers = ["akka.testkit.TestEventListener"]
    """
}

