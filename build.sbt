ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "RoverCommandPublisher"
  )

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
//    "org.slf4j" % "slf4j-api" % "2.0.5",
//    "ch.qos.logback" % "logback-classic" % "1.4.7",
  "org.scalatest" %% "scalatest" % "3.2.15"
)