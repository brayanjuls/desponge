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