# Rabbit-MQ HA Library [![Build Status](http://grisham:8111/app/rest/builds/buildType:%28id:Books_Platform_Hermes_RabbitmqHa_BuildTestPublish%29/statusIcon)](http://grisham:8111/viewType.html?buildTypeId=Books_Platform_Hermes_RabbitmqHa_BuildTestPublish&guest=1)

This library contains various helpers classes for interacting with RabbitMQ in a standard, reliable way.

There's a standalone example in [ConfirmedPublishingExample](src/test/scala/com/blinkboxbooks/hermes/rabbitmq/ConfirmedPublishingExample.scala) that uses a number of the classes in this library, this runs against a local RabbitMQ broker and aims to illustrate how failures are dealt with etc.

### Standard configuration for RabbitMQ

The library defines the class `RabbitMqConfig` which has values for the various settings needed to connect to RabbitMQ. The companion object also has a factory method that creates such configuration objects from standard Config objects as defined in the [common-config](/Platform/common-config) library, using specific parameters listed on the [Service configuration guidelines](http://jira.blinkbox.local/confluence/display/PT/Service+Configuration+Guidelines) page.

### Reliable broker connections

The class `RabbitMq` has a method `reliableConnection`, which will return a connection to the RabbitMQ broker that will automatically reconnect after failures, and restore the state of any Channels created on this connection.

This method takes the `RabbitMqConfig` object mentioned above as argument. Typically, applications will use this line of code to create a RabbitMq connection:

```scala
val connection = RabbitMq.reliableConnection(RabbitMqConfig(config))
```
where `config` is the configuration returned by the standard `Configuration` trait.

### Recovered broker connections

The `RabbitMq` class also has a method `recoveredConnection`, which will return a connection to the RabbitMQ broker that will automatically reconnect after failures, but that will **not** retry operations such as creating channels or publishing messages, which would then become blocking. This makes this connection suitable for use in Actors that for example publish messages on short-lived channels.

Typically, applications will create such RabbitMq connections as:

```scala
val connection = RabbitMq.recoveredConnection(RabbitMqConfig(config))
```
where `config` is the configuration returned by the standard `Configuration` trait.

### Common RabbitMQ message consumer

Use the `RabbitMqConsumer` actor class to retrieve messages from RabbitMQ, where the messages comply with our standard messaging guidelines.

Each instance of these is configured with a `QueueConfiguration` object, which can be constructed from standard `Config`. This takes the following values:

| Property name | Type    | Comment    
| ------ | ------- | ------- |
| queueName     | String | Name of queue.     |
| exchangeName  | String | Name of exchange queue is bound to.   |
| routingKeys   | List[String] | Optional list of routing keys used to bind queues to exchange, one binding per routing key. Ignored for fanout and header exchanges. |
| bindingArguments | Map | Optional value with key-value pairs used for headers binding |
| prefetchCount | Int | The maximum number of messages that can be in-process at once. |
| exchangeType  | String | The exchange type (topic, headers, etc). |

The consumer will declare exchanges, queues and bindings as needed, so that no manual setup is needed for RabbitMQ.


### General RabbitMQ message consumer

Use the `AmqpConsumerActor` class to retrieve messages from RabbitMQ. The messages produced by this actor directly pass on the content of the RabbitMQ message.

This class may be deprecated once all services publish messages in the standard format.

### Reliable message publisher

Use the `RabbitMqReliablePublisher` class to send message to RabbitMQ where it's critical that published messages are not lost. This actor will publish messages as persistent messages and using publisher confirms, and it will attempt to re-deliver messages in case of failures.

This actor does not currently give notifications back to senders whehter message publishing succeeds or not.

### Confirmed message publisher

Use the `RabbitMqConfirmedPublisher` class to send messages to RabbitMQ in the most reliable way available (as persistent messages and using Publisher Confirms), but reporting failures back to the sender instead of retrying. This is suitable for use where you need to send messages reliably, but want to handle failures yourself instead of having some other code retrying the publising for you. One example of such cases is if the message you're processing is the result of an incoming RabbitMQ messages that's stored in a persistent queue - in such cases it's often better to just retry that message later instead of ACKing it then having to make sure it's not lost down the line.

Each instance of this actor is configured with a `PublisherConfiguration` object, which can be constructed from standard `Config`. This takes the following values:

| Property name | Type    | Comment    
| ------ | ------- | ------- |
| messageTimeout | Duration | How long it will try before reporting a failure to publish a message. |
| exchangeName  | String | Name of exchange messages are published to. If a service wants to publish direct to a queue (not recommended practice) then this value can be ommitted, in which case the routingKey parameter identifies the name of the queue (this is using the RabbitMQ "Default Exchange" feature).  |
| routingKey   | String | Routing key used to route messages at the exchange published to. This value is ignored for some exchanges, e.g. fanout exchanges, in which case an empty value may be given. |
| prefetchCount | Int | The maximum number of messages that can be in-process at once. |
| exchangeType  | String | The exchange type (topic, headers, etc). |
| bindingArguments | Map | key-value pairs used as arguments when publishing a message|

The producer will declare exchanges, queues and bindings as needed, so that no manual setup is needed for RabbitMQ.
