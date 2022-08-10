ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "twitter",
    idePackagePrefix := Some("com.desponge")
  )

libraryDependencies ++= Seq(
  "com.danielasfregola" %% "twitter4s" % "8.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "org.apache.kafka" %% "kafka" % "2.8.1"
)

/// libraryDependencies += "com.twitter" % "twitter-api-java-sdk" % "2.0.2"
libraryDependencies += "org.scalatest"  %% "scalatest"  % "3.0.4"  % Test
libraryDependencies += "org.scalamock"  %% "scalamock"  % "4.1.0"  % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % Test

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.14.1")