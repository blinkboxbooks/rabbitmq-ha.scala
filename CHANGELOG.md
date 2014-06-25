# Change log

## 2.1.0 ([#7](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/7) 2014-06-23 17:37:22)

Added new RabbitMQ message consumer actor

### New features

Added new Akka Actor class for consuming messages from RabbitMQ. This new implementation have the following advantages w.r.t. the older AmqpConsumerActor class:

- It produces event messages as defined in the queue-neutral [common-messaging library](/Hermes/common-messaging), that contain a number of standard fields that all messages should contain (IDs, content types etc.).
- It uses the Cameo pattern for handling ACKs instead of the Ask pattern and futures.
- It is created with a configuration object that can be read from standard configuration, using the common-config library, and defines standard configuration properties for its settings. This will reduce the amount of boilerplate needed to use this code in services.
- The unit tests for the actor test it in isolation, and not together with the AmqpPublisherActor.

