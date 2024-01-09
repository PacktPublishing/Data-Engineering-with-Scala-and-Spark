package com.packt.dewithscala.chapter13

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  TimestampType
}
import org.apache.spark.sql.expressions.Window
import io.delta.tables._

object StreamingGold extends App {
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

  // Function to write DataFrame to a Delta Lake table
  private def writeDelta(df: DataFrame, tableName: String) = {
    df.write
      .format("delta")
      .mode("overwrite")
      .save(s"${target}${tableName}")
  }

  val silverSource: String =
    "./src/main/scala/com/packt/dewithscala/chapter13/data/silver/silver_devices"
  val target: String =
    "./src/main/scala/com/packt/dewithscala/chapter13/data/gold/"

  val silverData: DataFrame = spark.read.format("delta").load(silverSource)

  // Case 1: Count Event Types by Date
  val case1Df: DataFrame = silverData
    .groupBy(to_date($"event_ts").alias("event_date"), $"event_type")
    .agg(count($"event_type").alias("event_count"))
  writeDelta(
    case1Df,
    "event_by_date"
  )

  // Case 2: Lastest Device State

  private val windowSpec =
    Window.partitionBy("device_id").orderBy(desc("event_ts"))

  private val dfWindowedRank = silverData
    .withColumn("dense_rank", dense_rank().over(windowSpec))

  val case2Df: DataFrame = dfWindowedRank
    .filter("dense_rank = 1")
    .drop("dense_rank")

  writeDelta(
    case2Df,
    "latest_device_status"
  )
}
