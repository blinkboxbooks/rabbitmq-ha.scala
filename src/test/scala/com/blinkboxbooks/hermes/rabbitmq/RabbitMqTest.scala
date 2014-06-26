package com.blinkboxbooks.hermes.rabbitmq

import com.rabbitmq.client.Connection
import com.typesafe.config.{ ConfigFactory, ConfigException }
import java.net.URI
import net.jodah.lyra.config.ConfigurableConnection
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterEach, FunSuite }
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RabbitMqTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {

  var connection: Option[Connection] = None

  override def afterEach {
    connection.foreach(conn => if (conn.isOpen()) conn.close())
  }

  test("Create RabbitMQ config from default parameters") {
    val config = ConfigFactory.load("rabbitmq-config-url-only.conf")
    val parsedConfig = RabbitMqConfig(config)

    // Should pick up all settings from the given config.
    assert(parsedConfig.uri.toString == "amqp://guest:guest@foo.bar.com:1234" &&
      parsedConfig.initialRetryInterval == 2.seconds &&
      parsedConfig.maxRetryInterval == 10.seconds)
  }

  test("Create RabbitMQ config, overriding default values") {
    val config = ConfigFactory.load("rabbitmq-config-full.conf")
    val parsedConfig = RabbitMqConfig(config)

    // Should pick up the URI from the config and use the default values in reference.conf
    // for the rest.
    assert(parsedConfig.uri.toString == "amqp://guest:guest@foo.bar.com:1234" &&
      parsedConfig.initialRetryInterval == 5.seconds &&
      parsedConfig.maxRetryInterval == 42.seconds)
  }

  test("Create RabbitMQ config without required parameter") {
    val defaultConfig = ConfigFactory.load()
    intercept[ConfigException.Missing] { RabbitMqConfig(defaultConfig) }
  }

  // Can't easily test this without a real broker, hence this test is disabled.
  ignore("Create reliable connection") {
    val config = RabbitMqConfig(new URI("amqp://guest:guest@localhost:5672"), 1.second, 5.seconds)
    val conn = RabbitMq.reliableConnection(config)
    
    // Check that broker parameters have been picked up from the given test config.
    assert(conn.getAddress.getHostName == "localhost" &&
      conn.getPort == 5672)

    // Check that the other connection parameters come from the defaults in reference.conf.
    val lyraConnection = conn.asInstanceOf[ConfigurableConnection]
    assert(lyraConnection.getConnectionRecoveryPolicy.getInterval.toSeconds == 1 &&
      lyraConnection.getConnectionRecoveryPolicy.getMaxInterval.toSeconds == 5)
  }

}
