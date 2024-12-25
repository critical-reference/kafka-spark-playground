package org.example.application

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {

    val yellowTripDataPath = "C:\\Users\\elena\\Desktop\\NYCdata\\yellow_tripdata_2024-02.parquet"

    val schema = StructType(
      Seq(
        StructField("total_amount", DoubleType, true),
        StructField("trip_distance", DoubleType, true)
      )
    )

    val spark = SparkSession.builder
      .appName("SparkKafkaStructuredStreaming")
      .master("local[2]") // Use appropriate master in production
      .getOrCreate()

    val yellow_tripdata: DataFrame = spark.read
      .schema(schema)
      .parquet(yellowTripDataPath)

    val yellow_tripdata_simple = yellow_tripdata
      .select("total_amount", "trip_distance")

    yellow_tripdata_simple.show()

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:29092"
    val inputTopic = "input_topic"
    val outputTopic = "output_topic"

    val yellow_data = yellow_tripdata_simple
      .withColumn("key", lit("key"))
      .withColumn("value", struct($"total_amount", $"trip_distance"))

    val outputDF = yellow_data
      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")

    val query = outputDF.write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .save()



    val kafkaStream = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", outputTopic)
      .option("startingOffsets", "earliest")
      .load()

    val newData = kafkaStream
      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")

    newData.show(false)

    val parsedData = newData
      .withColumn("total_amount", split(regexp_replace($"value", "[{}]", ""), ",").getItem(0).cast(DoubleType))
      .withColumn("trip_distance", split(regexp_replace($"value", "[{}]", ""), ",").getItem(1).cast(DoubleType))
      //.select(from_json($"value", schema).as("data"))   // value is not json!
      //.select("data.total_amount", "data.trip_distance")

    parsedData.show()

    /*parsedData.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()*/




  }

}