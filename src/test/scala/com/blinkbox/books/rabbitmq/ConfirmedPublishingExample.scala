package com.blinkbox.books.rabbitmq

import akka.actor.{ Actor, ActorRef, ActorLogging, ActorSystem, Props }
import akka.actor.Status.{ Success, Failure }
import com.blinkbox.books.messaging.{ Event, EventHeader }
import com.blinkbox.books.rabbitmq.RabbitMqConsumer.QueueConfiguration
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.util.Random
import scala.xml.XML

/**
 * A simple ad-hoc test/example using RabbitMQ code for confirmed publishing,
 * that runs against a local RabbitMQ.
 *
 * You can for example try to kill and restart RabbitMQ while it's running to see how that's handled.
 */
object ConfirmedPublishingExample extends App {

  println("Starting")

  val QueueName = "test.confirmedPublishing.queue"
  val ExchangeName = "test.confirmedPublishing.exchange"
  val RoutingKey = "test.confirmedPublishing.routingKey"
  val exchangeType = "topic"

  def newConnection() = RabbitMq.reliableConnection(RabbitMqConfig(new URI("amqp://guest:guest@localhost:5672"), 2.seconds, 10.seconds))

  // Set up an actor that publishes messages every few seconds.
  {
    import RabbitMqConfirmedPublisher._

    val connection = newConnection()
    val system = ActorSystem("producer-system")
    val counter = new AtomicInteger()
    implicit val executionContext = system.dispatcher

    val publisher = system.actorOf(Props(
      new RabbitMqConfirmedPublisher(connection.createChannel(), PublisherConfiguration(Some(ExchangeName), Some(RoutingKey), None, 10.seconds, exchangeType))),
      name = "publisher")
    val responsePrinter = system.actorOf(Props(new ResponsePrinter()), name = "response-printer")

    // Send a steady stream of numbers every few seconds.
    system.scheduler.schedule(0.seconds, 3.seconds) {
      val newValue = counter.getAndIncrement
      val event = Event.xml(s"<value>$newValue</value>", EventHeader("test-producer"))
      publisher.tell(event, responsePrinter)
      println(s"Sent request with value $newValue")
    }
  }

  // Set up an actor that consumes messages and somewhat arbitrarily makes them succeed or fail.
  {
    val connection = newConnection()
    val system = ActorSystem("consumer-system")

    val output = system.actorOf(Props(new TestConsumer()), "test-consumer")
    val queueConfig = QueueConfiguration("test-queue", ExchangeName, Seq(RoutingKey), Map(), 50)
    val consumer = system.actorOf(Props(
      new RabbitMqConsumer(connection.createChannel(), queueConfig, "consumer-tag", output)), name = "rabbitmq-consumer")
    consumer ! RabbitMqConsumer.Init
  }

  println("Started!")

}

// An example actor that consumes numbers from XML messages and decides what it thinks of each.
class TestConsumer extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatcher

  def receive = {
    case e: Event =>
      val value = (XML.loadString(e.body.asString).text).toString.toInt
      log.info(s"Received value $value")
      if (isPrime(value)) respond(sender, Failure(new Exception(s"I hate primes like $value!!!")))
      else respond(sender, Success(s"Accepted value $value"))
  }

  // Respond after a random interval, just to be awkward.
  private def respond(to: ActorRef, response: Any) { context.system.scheduler.scheduleOnce(randomDelay, to, response) }
  private def randomDelay = Random.nextInt(3000).millis

  private def isPrime(num: Int) = num > 1 && !(2 until num).exists(num % _ == 0)
}

// Simple actor that just shows result of responses.
class ResponsePrinter extends Actor with ActorLogging {
  def receive = {
    case Success(result) => log.info(s"Success: $result")
    case Failure(e) => log.info(s"Failed: ${e.getMessage}")
  }
}

