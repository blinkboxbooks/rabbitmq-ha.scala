package com.blinkboxbooks.hermes.rabbitmq

import com.blinkbox.books.config.Configuration
import com.rabbitmq.client.{ Connection, ConnectionFactory }
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import net.jodah.lyra
import net.jodah.lyra.Connections
import net.jodah.lyra.config.{ RetryPolicies, RecoveryPolicy, RetryPolicy }
import net.jodah.lyra.util.{ Duration => LyraDuration }

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
   * The URL to the broker must always be provided as configuration.
   * The library comes with reference configuration that provides default settings for retry interval parameters,
   * which will be used unless the client's configuration explicitly specifies them.
   *
   */
  def reliableConnection(config: Config): Connection = {
    val initialRetryInterval = config.getDuration("rabbitmq.initialRetryInterval", TimeUnit.SECONDS)
    val maxRetryInterval = config.getDuration("rabbitmq.maxRetryInterval", TimeUnit.SECONDS)

    val factory = connectionFactory(config)
    val lyraConfig = new lyra.config.Config()
      .withRecoveryPolicy(new RecoveryPolicy()
        .withBackoff(LyraDuration.seconds(initialRetryInterval), LyraDuration.seconds(maxRetryInterval)))
      .withRetryPolicy(new RetryPolicy()
        .withBackoff(LyraDuration.seconds(initialRetryInterval), LyraDuration.seconds(maxRetryInterval)))

    Connections.create(factory, lyraConfig)
  }

  /**
   * Factory method for creating a RabbitMQ ConnectionFactory from configuration.
   */
  private def connectionFactory(config: Config) = {
    val uri = config.getString("rabbitmq.url")
    val factory = new ConnectionFactory()
    factory.setUri(uri)
    factory
  }

}
