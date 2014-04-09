name := "rabbitmq-ha"

version := "1.0.0"

organization := "com.blinkboxbooks.hermes"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"           % "2.3.0" withSources() withJavadoc(),
  "com.typesafe.akka" %% "akka-testkit"         % "2.3.0" withSources() withJavadoc(),
  "org.scalatest"     %% "scalatest"            % "1.9.1" % "test" withSources() withJavadoc(),
  "junit"              % "junit"                % "4.11" % "test" withSources() withJavadoc(),
  "com.rabbitmq"       % "amqp-client"          % "3.2.4" withSources() withJavadoc(),
  "net.jodah"          % "lyra"                 % "0.4.0" withSources() withJavadoc(),
  "org.mockito"        % "mockito-core"         % "1.9.5" % "test" withSources() withJavadoc()
)

scalacOptions ++= Seq("-feature", "-deprecation")

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

resolvers += "Akka Repo" at "http://repo.akka.io/releases"

parallelExecution := false

// Pick up login credentials for Nexus from user's directory.
credentials += Credentials(Path.userHome / ".sbt" / ".nexus")

publishTo := {
  val nexus = "http://nexus.mobcast.co.uk/"
  Some("Sonatype Nexus Repository Manager" at nexus + "nexus/content/repositories/releases")
}

