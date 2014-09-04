name := "rabbitmq-ha"

version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0")

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

organization := "com.blinkboxbooks.hermes"

crossScalaVersions := Seq("2.10.4", "2.11.2")

libraryDependencies ++= {
  val akkaV = "2.3.5"
  Seq(
    "com.blinkbox.books"  %%  "common-config"      %  "1.1.0",
    "com.blinkbox.books"  %%  "common-messaging"   %  "1.1.3",
    "com.typesafe.akka"   %%  "akka-actor"         %  akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"       %  akkaV,
    "com.typesafe.akka"   %%  "akka-slf4j"         %  akkaV,
    "com.rabbitmq"         %  "amqp-client"        %  "3.3.5",
    "net.jodah"            %  "lyra"               %  "0.4.1",
    "org.joda"             %  "joda-convert"       %  "1.6",
    "com.blinkbox.books"  %%  "common-scala-test"  %  "0.3.0"   % "test"
  )
}

parallelExecution := false
