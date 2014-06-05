name := "rabbitmq-ha"

version := scala.io.Source.fromFile("VERSION").mkString.trim

organization := "com.blinkboxbooks.hermes"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.2.3",
  "com.typesafe.akka" %% "akka-testkit" % "2.2.3",
  "com.rabbitmq"       % "amqp-client"  % "3.2.4",
  "net.jodah"          % "lyra"         % "0.4.0",
  "org.scalatest"     %% "scalatest"    % "1.9.1" % "test",
  "junit"              % "junit"        % "4.11" % "test",
  "org.mockito"        % "mockito-core" % "1.9.5" % "test"
)

scalacOptions ++= Seq("-feature", "-deprecation")

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

resolvers += "Akka Repo" at "http://repo.akka.io/releases"

parallelExecution := false

