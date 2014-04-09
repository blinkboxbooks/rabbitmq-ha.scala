package com.blinkboxbooks.hermes.rabbitmq

import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito._
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfter}
import akka.testkit.{TestActorRef, TestKit}
import akka.actor.ActorSystem
import akka.util.Timeout
import scala.concurrent.duration._
import com.rabbitmq.client.{ConfirmListener, MessageProperties, Envelope, Channel}
import org.mockito.{Matchers, ArgumentCaptor}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import com.rabbitmq.client.AMQP.BasicProperties
import java.io.IOException
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class AkkaAmqpActorsTests extends TestKit(ActorSystem("test-system")) with FunSuite with BeforeAndAfter {

  val queueName = "TEST_QUEUE"
  val amqpTimeout = Timeout(10 seconds)

  var channel: Channel = _
  var seqNo = 0L
  var confirmListenerArgument: ArgumentCaptor[ConfirmListener] = _
  var publisher: TestActorRef[AmqpPublisherActor] = _
  var consumer: TestActorRef[AmqpConsumerActor] = _

  def incAndGetSeqNo() = { seqNo += 1; seqNo }

  def getNewMessage = {
    val body = "some text"
    val envelope = new Envelope(1L, false, "", "")
    Message(s"consumer-tag", envelope, MessageProperties.MINIMAL_PERSISTENT_BASIC, body.getBytes)
  }

  trait TimeDilatedRetry extends AmqpRetry {
    override val RetryFactor = -1
    override val MaxAttempts = 4
  }

  before {
    seqNo = 0
    channel = mock(classOf[Channel])
    doAnswer(new Answer[Long] {
      override def answer(invocation: InvocationOnMock) = incAndGetSeqNo()
    }).when(channel).getNextPublishSeqNo

    confirmListenerArgument = ArgumentCaptor.forClass(classOf[ConfirmListener])

    publisher = TestActorRef(new AmqpPublisherActor(channel, queueName, amqpTimeout) with TimeDilatedRetry)
    verify(channel).addConfirmListener(confirmListenerArgument.capture())
    assert(confirmListenerArgument.getValue != null)
  }

  test("An acked message will not be resent") {
    publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    assert(publisher.underlyingActor.unconfirmedMessages.size == 1)

    confirmListenerArgument.getValue.handleAck(1L, false)
    assert(publisher.underlyingActor.unconfirmedMessages.isEmpty)

    verify(channel, times(1)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("Multiple acked messages will not be resent") {
    for (i <- 1 to 3) {
      publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    }
    assert(publisher.underlyingActor.unconfirmedMessages.size == 3)

    confirmListenerArgument.getValue.handleAck(3L, true)
    assert(publisher.underlyingActor.unconfirmedMessages.isEmpty)

    verify(channel, times(3)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("A nacked message will be resent") {
    publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    assert(publisher.underlyingActor.unconfirmedMessages.size == 1)

    confirmListenerArgument.getValue.handleNack(1L, false)

    confirmListenerArgument.getValue.handleAck(2L, false)
    assert(publisher.underlyingActor.unconfirmedMessages.isEmpty)

    verify(channel, times(2)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("Multiple nacked messages will be resent") {
    for (i <- 1 to 3) {
      publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    }
    assert(publisher.underlyingActor.unconfirmedMessages.size == 3)

    confirmListenerArgument.getValue.handleNack(3L, true)

    confirmListenerArgument.getValue.handleAck(6L, true)
    assert(publisher.underlyingActor.unconfirmedMessages.isEmpty)

    verify(channel, times(6)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("A message that failed to be sent will be resent") {
    // throw an exception the first time
    doThrow(new IOException).
      doNothing().
      when(channel).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
        Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))

    publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    assert(publisher.underlyingActor.unconfirmedMessages.size == 1)

    confirmListenerArgument.getValue.handleAck(2L, false)
    assert(publisher.underlyingActor.unconfirmedMessages.isEmpty)

    verify(channel, times(2)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("Multiple messages that failed to be sent will be resent") {
    // throw an exception the first three times
    doThrow(new IOException).
      doThrow(new IOException).
      doThrow(new IOException).
      doNothing().
      when(channel).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
        Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))

    for (i <- 1 to 3) {
      publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    }
    assert(publisher.underlyingActor.unconfirmedMessages.size === 3)

    confirmListenerArgument.getValue.handleAck(6L, true)
    assert(publisher.underlyingActor.unconfirmedMessages.isEmpty)

    verify(channel, times(6)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("A message that failed will be retried 4 times") {
    doThrow(new IOException).
      doThrow(new IOException).
      doThrow(new IOException).
      doNothing().
      when(channel).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
        Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))

    publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    assert(1 === publisher.underlyingActor.unconfirmedMessages.size)

    confirmListenerArgument.getValue.handleAck(4L, false)
    assert(0 === publisher.underlyingActor.unconfirmedMessages.size)

    verify(channel, times(4)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }

  test("A message that failed will be retried 4 times with the final failure outcome") {
    doThrow(new IOException).
      doThrow(new IOException).
      doThrow(new IOException).
      doNothing().
      when(channel).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
        Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))

    publisher ! AmqpPublisherActor.PublishRequest(getNewMessage)
    assert(1 === publisher.underlyingActor.unconfirmedMessages.size)

    confirmListenerArgument.getValue.handleNack(4L, false)
    assert(0 === publisher.underlyingActor.unconfirmedMessages.size)

    verify(channel, times(4)).basicPublish(Matchers.any(classOf[String]), Matchers.eq(queueName),
      Matchers.any(classOf[BasicProperties]), Matchers.any(classOf[Array[Byte]]))
  }
}
