import _root_.sbt.Keys._
import _root_.scala._

name := "rabbitmq-ha"

version := "1.0.0"

organization := "com.blinkboxbooks.hermes"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"           % "2.3.0" withSources() withJavadoc(),
  "com.typesafe.akka" %% "akka-testkit"         % "2.3.0" withSources() withJavadoc(),
  "io.spray"           % "spray-client"         % "1.2-20131106" withSources() withJavadoc(),
  "io.spray"           % "spray-http"           % "1.2-20131106" withSources() withJavadoc(),
  "io.spray"           % "spray-httpx"          % "1.2-20131106" withSources() withJavadoc(),
  "io.spray"          %% "spray-json"           % "1.2.5" withSources() withJavadoc(),
  "com.typesafe"      %% "scalalogging-slf4j"   % "1.0.1" withSources() withJavadoc(),
  "org.slf4j"          % "slf4j-log4j12"        % "1.7.5" withSources() withJavadoc(),
  "log4j"              % "log4j"                % "1.2.17" withSources() withJavadoc(),
  "org.scalatest"     %% "scalatest"            % "1.9.1" % "test" withSources() withJavadoc(),
  "junit"              % "junit"                % "4.11" % "test" withSources() withJavadoc(),
  "com.novocode"       % "junit-interface"      % "0.10" % "test" withSources() withJavadoc(),
  "com.rabbitmq"       % "amqp-client"          % "3.2.3" withSources() withJavadoc(),
  "net.jodah"          % "lyra"                 % "0.4.0" withSources() withJavadoc(),
  "org.squeryl"       %% "squeryl"              % "0.9.5-6",
  "com.h2database"     % "h2"                   % "1.3.173" % "test" withSources() withJavadoc(),
  "mysql"              % "mysql-connector-java" % "5.1.26",
  "org.scalaequals"   %% "scalaequals-core"     % "1.2.0",
  "commons-dbcp"       % "commons-dbcp"         % "1.4",
  "com.typesafe"       % "config"               % "1.0.2",
  "org.mockito"        % "mockito-core"         % "1.9.5" % "test" withSources() withJavadoc(),
  "org.json4s"        %% "json4s-jackson"       % "3.2.5"
)

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

scalacOptions ++= Seq("-feature", "-deprecation")

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

resolvers += "SpringSource Enterprise Bundle Repository" at "http://nexus.mobcast.co.uk/nexus/content/repositories/sebr-external-bundle-releases"

resolvers += "Akka Repo" at "http://repo.akka.io/releases"

resolvers += "spray repo" at "http://repo.spray.io"

resolvers += "spray nightlies repo" at "http://nightlies.spray.io"

parallelExecution := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

// Leave out bits we don't care about.
publishArtifact in (Compile, packageDoc) := false

publishArtifact in (Compile, packageSrc) := false

// Pick up login credentials for Nexus from user's directory.
credentials += Credentials(Path.userHome / ".sbt" / ".nexus")

publishTo := {
  val nexus = "http://jenkins:m0bJenk@nexus.mobcast.co.uk/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("Sonatype Nexus Repository Manager" at nexus + "nexus/content/repositories/snapshots/")
  else
    Some("Sonatype Nexus Repository Manager"  at nexus + "nexus/content/repositories/releases")
}

