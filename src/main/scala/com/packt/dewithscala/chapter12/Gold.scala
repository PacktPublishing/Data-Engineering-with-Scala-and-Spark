package com.packt.dewithscala.chapter12

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.log4j.{Level, Logger}

object Gold extends App {
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

  val silverCE: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/silver/conversion_events"

  val target: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/gold/"

  val silverConversionEvents: DataFrame =
    spark.read.format("delta").load(silverCE)

  val goldConversionEvents: DataFrame =
    silverConversionEvents
      .groupBy(
        col("country_id"),
        col("campaign_id"),
        col("conversion_event_id"),
        col("date")
      )
      .agg(
        count(col("conversion_event")).alias("event_count")
      )

  goldConversionEvents.write
    .format("delta")
    .mode("overwrite")
    .save(s"${target}conversion_events")

}
