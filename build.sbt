ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-spark-playgroud",
    idePackagePrefix := Some("org.example.application")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  "org.apache.spark" %% "spark-streaming" % "3.5.1",
  "org.apache.kafka" % "kafka-clients" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1")