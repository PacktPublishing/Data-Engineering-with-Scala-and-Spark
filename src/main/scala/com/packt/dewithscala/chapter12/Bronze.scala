package com.packt.dewithscala.chapter12

import org.apache.spark.sql._

import org.apache.log4j.{Level, Logger}

object Bronze extends App {
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

  // Local
  val source: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/"
  val target: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/bronze/"

  // Function to read JSON data for a given date
  def readJson(date: String): DataFrame = {
    spark.read.json(s"${source}event_date=${date}/")
  }

  // Function to write DataFrame to a Delta Lake table
  private def writeDelta(reprocess: Boolean, df: DataFrame) = {
    df.write
      .format("delta")
      .mode(reprocess match {
        case false => "append"
        case _     => "overwrite"
      })
      .save(s"${target}conversion_events")
  }

  // Function to ingest and union JSON data for multiple dates
  private def ingest(reprocess: Boolean, dates: String*) = {
    dates.drop(1).foldLeft(readJson(dates.headOption.getOrElse("1900-01-01"))) {
      (acc, date) =>
        acc union readJson(date)
    }
  }

  // The first parameter should be a boolean, 2...n are dates to be processed in format yyyy-mm-dd
  val reprocess: Boolean   = args(0).toBoolean
  val dates: Array[String] = args.tail

  writeDelta(reprocess, ingest(reprocess, args.tail: _*))

}
