package com.blinkboxbooks.hermes.rabbitmq

import com.rabbitmq.client.Connection
import com.typesafe.config.{ ConfigFactory, ConfigException }
import net.jodah.lyra.config.ConfigurableConnection
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterEach, FunSuite }
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import RabbitMq._

@RunWith(classOf[JUnitRunner])
class RabbitMqTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {

  var connection: Option[Connection] = None

  override def afterEach {
    connection.foreach(conn => if (conn.isOpen()) conn.close())
  }

  // Can't easily test this without a real broker, hence this test is disabled.
  ignore("Create connection from default parameters") {
    // Create configuration that contains the URL in "rabbitmq.url".
    val config = ConfigFactory.load("rabbitmq-config-url-only.conf")
    connection = Some(reliableConnection(config))
    connection.foreach { conn =>
      // Check that broker parameters have been picked up from the given test config.
      assert(conn.getAddress.getHostName == "localhost" &&
        conn.getPort == 5672)

      // Check that the other connection parameters come from the defaults in reference.conf.
      val lyraConnection = conn.asInstanceOf[ConfigurableConnection]
      assert(lyraConnection.getConnectionRecoveryPolicy.getInterval.toSeconds == 2 &&
        lyraConnection.getConnectionRecoveryPolicy.getMaxInterval.toSeconds == 10)
    }
  }

  // Can't easily test this without a real broker, hence this test is disabled.
  ignore("Create connection from default parameters, overriding default values") {
    // Create configuration that contains the URL in "rabbitmq.url".
    val config = ConfigFactory.load("rabbitmq-config-full.conf")
    connection = Some(reliableConnection(config))

    connection.foreach { conn =>
      // Now all parameters should have been got from the given test config.
      assert(conn.getAddress.getHostName == "localhost" &&
        conn.getPort == 5672)
      val lyraConnection = conn.asInstanceOf[ConfigurableConnection]
      assert(lyraConnection.getConnectionRecoveryPolicy.getInterval.toSeconds == 5 &&
        lyraConnection.getConnectionRecoveryPolicy.getMaxInterval.toSeconds == 42)
    }
  }

  test("Try to create connection without required parameter") {
    val defaultConfig = ConfigFactory.load()
    intercept[ConfigException.Missing] { reliableConnection(defaultConfig) }
  }

}
