package com.blinkbox.books.rabbitmq

import com.blinkbox.books.config.RichConfig
import com.rabbitmq.client.{ Connection, ConnectionFactory, PossibleAuthenticationFailureException }
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.net.URI
import java.util.concurrent.TimeUnit
import net.jodah.lyra
import net.jodah.lyra.Connections
import net.jodah.lyra.config.{ Config => LyraConfig, RetryPolicies, RecoveryPolicy, RetryPolicy }
import net.jodah.lyra.util.{ Duration => LyraDuration }
import scala.annotation.tailrec
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
object RabbitMq extends StrictLogging {

  /**
   * Factory method for creating a reliable connection to a RabbitMQ broker.
   *
   * This connection will be reliable, in that it will automatically reconnect and re-initialise after
   * a broker failure. This includes when first connecting to the broker.
   * It will also retry any failed actions on created Channels, such as publishing.
   * Note that retrying of such actions will cause API operations to block.
   *
   * @param config A configuration object that contains the settings needed for connecting
   * to RabbitMQ.
   *
   */
  def reliableConnection(config: RabbitMqConfig): Connection = {
    val factory = connectionFactory(config)
    val lyraConfig = new LyraConfig()
      .withRecoveryPolicy(new RecoveryPolicy()
        .withBackoff(toDuration(config.initialRetryInterval), toDuration(config.maxRetryInterval)))
      .withRetryPolicy(new RetryPolicy()
        .withBackoff(toDuration(config.initialRetryInterval), toDuration(config.maxRetryInterval)))

    createConnection(factory, lyraConfig, config.initialRetryInterval)
  }

  /**
   * Factory method for creating a recovered connection to a RabbitMQ broker.
   *
   * This connection will be reliable, in that it will automatically reconnect and re-initialise after
   * a broker failure. This includes when first connecting to the broker.
   * It will however NOT recover created Channels after failures, nor will it retry any actions on Channels.
   * Instead it will return error on these operations directly to the caller.
   *
   * This avoids blocking on actions such as creating short-lived channels or publishing messages,
   * which is important when performing such actions within Actors.
   *
   * @param config A configuration object that contains the settings needed for connecting
   * to RabbitMQ.
   */
  def recoveredConnection(config: RabbitMqConfig): Connection = {
    val factory = connectionFactory(config)
    // Recover connections, but do no other error handling or retry/recovery.
    val lyraConfig = new lyra.config.Config()
      .withConnectionRecoveryPolicy(new RecoveryPolicy()
        .withBackoff(toDuration(config.initialRetryInterval), toDuration(config.maxRetryInterval)))

    createConnection(factory, lyraConfig, config.initialRetryInterval)
  }

  private def createConnection(connectionFactory: ConnectionFactory, lyraConfig: LyraConfig, retryInterval: FiniteDuration) =
    retryIfAuthFails(retryInterval) { Connections.create(connectionFactory, lyraConfig) }

  /**
   * Retry the given operation in face of an authentication failure,
   * and pass on any other exceptions thrown.
   */
  @tailrec
  def retryIfAuthFails[T](retryInterval: FiniteDuration)(op: => T): T =
    try op
    catch {
      case e: PossibleAuthenticationFailureException =>
        logger.warn("Possible authentication failure, retrying")
        Thread.sleep(retryInterval.toMillis)
        retryIfAuthFails(retryInterval)(op)
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
