package part4integrationsmy

import org.apache.spark.sql.functions.{callUDF, col, expr, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import common._

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("IntegratingKafka")
    .master("local[4]")
    .getOrCreate()

  def readFromKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without ckeckpoints the writing to Kafka will fail
      .start()
      .awaitTermination()
  }

  /**
   * Ecercise: write the whole cars data structures to Kafka as JSON.
   * Use struct columns an the to_json function.
   */
  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin")))
        .cast("String")
        .as("value")
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without ckeckpoints the writing to Kafka will fail
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    writeCarsToKafka()

  }
}
