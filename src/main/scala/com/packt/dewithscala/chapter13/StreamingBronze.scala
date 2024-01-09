package com.packt.dewithscala.chapter13

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

object StreamingBronze extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("dewithscala")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.legacy.timeParserPolicy", "Legacy")
    .getOrCreate()

  import spark.implicits._

  val target: String =
    "./src/main/scala/com/packt/dewithscala/chapter13/data/bronze/"

  val readConnectionString =
    "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=policy1;SharedAccessKey=<key>;EntityPath=dewithscala"
  val topicName       = "dewithscala"
  val ehNamespaceName = "<namespace>"
  val ehSasl =
    "org.apache.kafka.common.security.plain.PlainLoginModule" ++
      " required username='$ConnectionString' password='" ++ readConnectionString ++ "';"
  val bootstrapServers = s"$ehNamespaceName.servicebus.windows.net:9093"

  val df = spark.readStream
    .format("kafka")
    .option("subscribe", topicName)
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", ehSasl)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .option("failOnDataLoss", "true")
    .option("startingOffsets", "latest") // latest
    .load()

  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", s"$target/_checkpoints/")
    .option("path", s"$target/bronze/data/")
    .start()
    .awaitTermination()

}
