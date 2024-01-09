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
import io.delta.tables._

object StreamingSilver extends App {
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

  val reprocess: Boolean   = args(0).toBoolean

  val bronzeSource: String =
    "./src/main/scala/com/packt/dewithscala/chapter13/data/bronze/bronze/data/"

  val target: String =
    "./src/main/scala/com/packt/dewithscala/chapter13/data/silver/"

  val bronzeData: DataFrame = spark.read.format("delta").load(bronzeSource)

  val jsonSchema: StructType = StructType(
    Seq(
      StructField("device_id", StringType),
      StructField("country", StringType),
      StructField("event_type", StringType),
      StructField("event_ts", TimestampType)
    )
  )

  val updateSilver: DataFrame = bronzeData
    .select(from_json(col("value"), jsonSchema).alias("value"))
    .select(
      col("value.device_id"),
      col("value.country"),
      col("value.event_type"),
      col("value.event_ts")
    )
    .dropDuplicates("device_id", "country", "event_ts")

  reprocess match {
    case true =>
      updateSilver.write
        .format("delta")
        .mode("overwrite")
        .save(s"${target}silver_devices")
    case _ => {
      val silverTarget = DeltaTable.forPath(spark, s"{$target}silver_devices")

      silverTarget
        .as("devices")
        .merge(
          updateSilver.as("update"),
          "devices.device_id = update.device_id AND devices.country = update.country AND devices.event_ts = update.event_ts"
        )
        .whenNotMatched()
        .insertAll()
        .execute()
    }
  }

}
