name := "rabbitmq-ha"

version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0")

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

organization := "com.blinkbox.books.hermes"

crossScalaVersions := Seq("2.10.4", "2.11.2")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  Seq(
    "com.blinkbox.books"  %%  "common-config"      %  "1.4.1",
    "com.blinkbox.books"  %%  "common-messaging"   %  "1.1.5",
    "com.typesafe.akka"   %%  "akka-actor"         %  akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"       %  akkaV,
    "com.typesafe.akka"   %%  "akka-slf4j"         %  akkaV,
    "com.rabbitmq"         %  "amqp-client"        %  "3.3.5",
    "net.jodah"            %  "lyra"               %  "0.4.1",
    "com.blinkbox.books"  %%  "common-scala-test"  %  "0.3.0"   % "test"
  )
}

parallelExecution := false
