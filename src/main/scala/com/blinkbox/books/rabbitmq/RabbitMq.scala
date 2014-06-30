package com.blinkbox.books.rabbitmq

import com.blinkbox.books.config.RichConfig
import com.rabbitmq.client.{ Connection, ConnectionFactory }
import com.typesafe.config.Config
import java.net.URI
import java.util.concurrent.TimeUnit
import net.jodah.lyra
import net.jodah.lyra.Connections
import net.jodah.lyra.config.{ RetryPolicies, RecoveryPolicy, RetryPolicy }
import net.jodah.lyra.util.{ Duration => LyraDuration }
import scala.concurrent.duration._

/**
 * Object that encapsulates configuration values for a standard RabbitMQ connection.
 */
case class RabbitMqConfig(uri: URI, initialRetryInterval: FiniteDuration, maxRetryInterval: FiniteDuration)

object RabbitMqConfig {

  /**
   * Create RabbitMQ configuration from configuration.
   *
   * The URL to the broker must always be provided as configuration.
   * The library comes with reference configuration that provides default settings for retry interval parameters,
   * which will be used unless the client's configuration explicitly specifies them.
   */
  def apply(config: Config): RabbitMqConfig = {
    val initialRetryInterval = config.getDuration("rabbitmq.initialRetryInterval", TimeUnit.SECONDS)
    val maxRetryInterval = config.getDuration("rabbitmq.maxRetryInterval", TimeUnit.SECONDS)
    RabbitMqConfig(config.getUri("rabbitmq.url", "amqp"), initialRetryInterval.seconds, maxRetryInterval.seconds)
  }
}

/**
 * A collection of common functionality for simplifying access to RabbitMQ.
 */
object RabbitMq {

  /**
   * Factory method for creating a reliable connection to a RabbitMQ broker.
   *
   * This connection will be reliable, in that it will automatically reconnect and re-initialise after
   * a broker failure. This includes when first connecting to the broker.
   *
   * @param config A configuration object that contains the settings needed for connecting
   * to RabbitMQ.
   *
   */
  def reliableConnection(config: RabbitMqConfig): Connection = {
    val factory = connectionFactory(config)
    val lyraConfig = new lyra.config.Config()
      .withRecoveryPolicy(new RecoveryPolicy()
        .withBackoff(toDuration(config.initialRetryInterval), toDuration(config.maxRetryInterval)))
      .withRetryPolicy(new RetryPolicy()
        .withBackoff(toDuration(config.initialRetryInterval), toDuration(config.maxRetryInterval)))

    Connections.create(factory, lyraConfig)
  }

  /** Convert between Scala and Lyra duration types. */
  private def toDuration(duration: FiniteDuration): lyra.util.Duration = LyraDuration.seconds(duration.toSeconds)

  /**
   * Factory method for creating a RabbitMQ ConnectionFactory from configuration.
   */
  private def connectionFactory(config: RabbitMqConfig) = {
    val factory = new ConnectionFactory()
    factory.setUri(config.uri)
    factory
  }

}
