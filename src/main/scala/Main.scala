package org.example.application

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkKafkaStructuredStreaming")
      .master("local[*]") // Use appropriate master in production
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:9092" // Replace with your Kafka bootstrap servers
    val inputTopic = "input-topic"
    val outputTopic = "output-topic"

    val kafkaInputDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest") // Options: earliest, latest, specific offsets
      .load()


    val kafkaData = kafkaInputDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    case class Record(id: Int, name: String, timestamp: String)

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("timestamp", StringType, true)
      )
    )

    val parsedDF = kafkaData
      .select(from_json($"value", schema).as("data"))
      .select("data.*")

    val filteredDF = parsedDF.filter($"id" > 100)

    val outputDF = filteredDF.selectExpr(
      "CAST(id AS STRING) AS key",
      "CAST(name AS STRING) AS value"
    )

    val query = outputDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      //.option("checkpointLocation", "/path/to/checkpoint/dir") // Necessary for fault tolerance
      .start()
    query.awaitTermination()
  }
}