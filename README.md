# Rabbit-MQ HA Library [![Build Status](http://teamcity01.mobcastdev.local:8111/app/rest/builds/buildType:%28id:Hermes_RabbitMQ_HA_BuildPublish%29/statusIcon)](http://teamcity01.mobcastdev.local:8111/viewType.html?buildTypeId=Hermes_RabbitMQ_HA_BuildPublish&guest=1)

This library contains various helpers classes for interacting with RabbitMQ in a standard, reliable way.

### Standard configuration for RabbitMQ

The library defines the class `RabbitMqConfig` which has values for the various settings needed to connect to RabbitMQ. The companion object also has a factory method that creates such configuration objects from standard Config objects as defined in the [common-config](/Platform/common-config) library, using specific parameters listed on the [Service configuration guidelines](http://jira.blinkbox.local/confluence/display/PT/Service+Configuration+Guidelines) page.

### Reliable broker connections

The class RabbitMq has a method `reliableConnection`, which will return a connection to the RabbitMQ broker that will automatically reconnect after failures, and restore the state of any Channels created on this connection.

This method takes the `RabbitMqConfig` object mentioned above as argument. Typically, applications will use this line of code to create a RabbitMq connection:

```scala
val connection = RabbitMq.reliableConnection(RabbitMqConfig(config))
```
where `config` is the configuration returned by the standard `Configuration` trait.

### Common RabbitMQ message consumer

Use the RabbitMqConsumer actor class to retrieve messages from RabbitMQ, where the messages comply with our standard messaging guidelines.


### General RabbitMQ message consumer

Use AmqpConsumerActor to retrieve messages from RabbitMQ. The messages produced by this actor directly pass on the content of the RabbitMQ message.

This class may be deprecated once all services publish messages in the standard format.

### Reliable message publisher

Use AmqpPublisherActor to send message to RabbitMQ where it's critical that published messages are not lost. This actor will publish messages as persistent messages and using publisher confirms, and it will attempt to re-deliver messages in case of failures.

