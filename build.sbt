import _root_.sbt.Keys._
import _root_.scala._

name := "rabbitmq-ha"

version := "1.0.0"

organization := "com.blinkboxbooks.hermes"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"           % "2.3.0" withSources() withJavadoc(),
  "com.typesafe.akka" %% "akka-testkit"         % "2.3.0" withSources() withJavadoc(),
  "com.typesafe"      %% "scalalogging-slf4j"   % "1.0.1" withSources() withJavadoc(),
  "org.slf4j"          % "slf4j-log4j12"        % "1.7.5" withSources() withJavadoc(),
  "log4j"              % "log4j"                % "1.2.17" withSources() withJavadoc(),
  "org.scalatest"     %% "scalatest"            % "1.9.1" % "test" withSources() withJavadoc(),
  "junit"              % "junit"                % "4.11" % "test" withSources() withJavadoc(),
  "com.rabbitmq"       % "amqp-client"          % "3.2.3" withSources() withJavadoc(),
  "net.jodah"          % "lyra"                 % "0.4.0" withSources() withJavadoc(),
  "org.scalaequals"   %% "scalaequals-core"     % "1.2.0",
  "com.typesafe"       % "config"               % "1.0.2",
  "org.mockito"        % "mockito-core"         % "1.9.5" % "test" withSources() withJavadoc()
)

scalacOptions ++= Seq("-feature", "-deprecation")

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

resolvers += "Akka Repo" at "http://repo.akka.io/releases"

parallelExecution := false

// Pick up login credentials for Nexus from user's directory.
credentials += Credentials(Path.userHome / ".sbt" / ".nexus")

publishTo := {
  val nexus = "http://jenkins:m0bJenk@nexus.mobcast.co.uk/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("Sonatype Nexus Repository Manager" at nexus + "nexus/content/repositories/snapshots/")
  else
    Some("Sonatype Nexus Repository Manager"  at nexus + "nexus/content/repositories/releases")
}

