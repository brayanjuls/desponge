name := "twitter_client"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.desponge")

resolvers += Resolver.sonatypeRepo("releases")
resolvers += "Confluent Repo" at "https://packages.confluent.io/maven"
resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"

libraryDependencies ++= Seq(
 "com.danielasfregola" %% "twitter4s" % "7.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
 "org.apache.kafka" % "kafka-clients" % "2.6.0",
 "org.apache.kafka" %% "kafka" % "2.8.1",
 "org.scalactic" %% "scalactic" % "3.2.10",
 "org.scalatest" %% "scalatest" % "3.2.10" % "test"
)

val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
 "io.circe" %% "circe-core",
 "io.circe" %% "circe-generic",
 "io.circe" %% "circe-parser"
).map(_ % circeVersion)