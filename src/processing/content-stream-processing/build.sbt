ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

val sparkVersion = "3.2.0"
lazy val root = (project in file("."))
  .settings(
    name := "content-stream-processing"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.1"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.1"
libraryDependencies += "org.scalatest"  %% "scalatest"  % "3.0.4"  % Test
libraryDependencies += "org.scalamock"  %% "scalamock"  % "4.1.0"  % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % Test




