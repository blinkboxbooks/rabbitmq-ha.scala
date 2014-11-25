lazy val root = (project in file(".")).
  settings(
    name := "rabbitmq-ha",
    organization := "com.blinkbox.books.hermes",
    version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0"),
    scalaVersion := "2.11.4",
    crossScalaVersions := Seq("2.11.4"),
    scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7", "-Xfatal-warnings", "-Xfuture"),
    libraryDependencies ++= {
      val akkaV = "2.3.7"
      Seq(
        "com.blinkbox.books"  %%  "common-config"      %  "2.0.1",
        "com.blinkbox.books"  %%  "common-messaging"   %  "2.0.0",
        "com.typesafe.akka"   %%  "akka-actor"         %  akkaV,
        "com.typesafe.akka"   %%  "akka-testkit"       %  akkaV,
        "com.typesafe.akka"   %%  "akka-slf4j"         %  akkaV,
        "com.rabbitmq"         %  "amqp-client"        %  "3.4.1",
        "net.jodah"            %  "lyra"               %  "0.4.3",
        "com.blinkbox.books"  %%  "common-scala-test"  %  "0.3.0"   %  Test
      )
    },
    parallelExecution := false
  )
