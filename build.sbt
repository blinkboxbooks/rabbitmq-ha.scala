name := "rabbitmq-ha"

version := scala.io.Source.fromFile("VERSION").mkString.trim

organization := "com.blinkbox.books.hermes"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.blinkbox.books" %% "common-config" % "0.7.1",
  "com.blinkbox.books" %% "common-messaging" % "0.4.0",
  "com.typesafe.akka" %% "akka-actor"   % "2.3.3",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.3",
  "com.rabbitmq"       % "amqp-client"  % "3.3.2",
  "net.jodah"          % "lyra"         % "0.4.1",
  "org.joda"           % "joda-convert" % "1.6",
  "com.typesafe.akka" %% "akka-slf4j" % "2.3.3",
  "org.scalatest"     %% "scalatest"    % "2.2.0" % "test",
  "junit"              % "junit"        % "4.11" % "test",
  "org.mockito"        % "mockito-core" % "1.9.5" % "test"
)

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

parallelExecution := false

