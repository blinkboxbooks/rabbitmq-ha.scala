# Change log

## 3.0.3 ([#12](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/12) 2014-07-11 15:09:31)

Changed default xml content type header to received message's content type 

### Bug Fix

* changed default xml content type to messages' content type

## 3.0.2 ([#11](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/11) 2014-07-02 11:52:43)

Bug fixes after integration testing with RabbitMQ.

Patch release that fixes the following problems:

- Fix queue bindings in Consumer actor so that queues that are not bound to topic exchanges will still be automatically bound.
- Don't fail on incoming messages that haven't got a timestamp, in order to cope with existing services and messages.
- Don't use RabbitMQ userId field as that has a specific meaning, define our own user ID header instead.
- Ensure child actor names for publisher are unique.
- Remove use of PurchaseRequest() wrapper, just send Event objects to Publisher.
- Improved logging.
- Better tests.


## 3.0.1 ([#10](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/10) 2014-07-01 13:42:09)

Fix declaration of exchanges and queues

Patch that fixes declarations of queues and exchanges to cover consuming messages from manually configured exchanges, and publishing direct to queues.

## 3.0.0 ([#9](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/9) 2014-06-27 15:49:20)

Added actor class for message publishing with Success/Failure confirmations

#### Breaking changes

- Renamed `AmqpPublisherActor` to `RabbitMqReliablePublisher`, to distinguish it from the new publisher actor.
- Changed package names from com.blinkboxbooks.hermes.rabbitmq to com.blinkbox.books.rabbitmq, for consistency with other projects.

#### New features:

- Added `RabbitMqConfirmedPublisher` actor class for publishing messages to RabbitMQ in a reliable fashion, with a configured timeout, where the client will receive a Success or Failure response.


## 2.2.0 ([#8](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/8) 2014-06-25 17:18:27)

Added API for creating reliable connections in a standard, configurable way.

#### New features

### Connection creation

There's a new API call for creating RabbitMQ connections. The purpose of this is to:

- Remove boilerplate code for creating RabbitMQ connections in the "right" way from services.
- Ensure each service gets the configuration for RabbitMQ from a standard place, as specified at http://jira.blinkbox.local/confluence/display/PT/Service+Configuration+Guidelines
- Avoid duplication of code, meaning that if (when!) we find a better/more reliable way of creating connections, we only have to change code in one place.


## 2.1.0 ([#7](https://git.mobcastdev.com/Hermes/rabbitmq-ha/pull/7) 2014-06-23 17:37:22)

Added new RabbitMQ message consumer actor

### New features

Added new Akka Actor class for consuming messages from RabbitMQ. This new implementation have the following advantages w.r.t. the older AmqpConsumerActor class:

- It produces event messages as defined in the queue-neutral [common-messaging library](/Hermes/common-messaging), that contain a number of standard fields that all messages should contain (IDs, content types etc.).
- It uses the Cameo pattern for handling ACKs instead of the Ask pattern and futures.
- It is created with a configuration object that can be read from standard configuration, using the common-config library, and defines standard configuration properties for its settings. This will reduce the amount of boilerplate needed to use this code in services.
- The unit tests for the actor test it in isolation, and not together with the AmqpPublisherActor.

