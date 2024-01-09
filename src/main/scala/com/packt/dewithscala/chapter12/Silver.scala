package com.packt.dewithscala.chapter12

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.log4j.{Level, Logger}

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.Check

object Silver extends App {
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
  val sourceCE: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/bronze/conversion_events"
  val sourceDimCE: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/conversion_events.csv"
  val sourceDimCountry: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/country_table.csv"
  val sourceDimCampaigns: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/marketing_campaigns.csv"

  val target: String =
    "./src/main/scala/com/packt/dewithscala/chapter12/data/silver/"

  def read(formatType: String, sourceLocation: String): DataFrame = {
    formatType match {
      case "delta" => spark.read.format(formatType).load(s"${sourceLocation}")
      case "csv" =>
        spark.read
          .format(formatType)
          .options(Map("delimiter" -> ",", "header" -> "true"))
          .load(s"${sourceLocation}")
    }

  }

  val bronzeConversionEvents: DataFrame = read("delta", sourceCE)
  val dimConversionEvents: DataFrame    = read("csv", sourceDimCE)
  val dimCountry: DataFrame             = read("csv", sourceDimCountry)
  val dimCampaigns: DataFrame           = read("csv", sourceDimCampaigns)

  val explodedBronzeCE: DataFrame = bronzeConversionEvents
    .withColumn("date", to_date(col("event_info.event_ts")))
    .withColumn("event_type", col("event_info.event_type"))

  val joinedData: DataFrame = explodedBronzeCE
    .join(
      dimConversionEvents,
      trim(lower(col("conversion_event"))) === trim(lower(col("event_type"))),
      "left"
    )
    .join(dimCountry, col("iso_2_char") === col("country"), "left")
    .join(
      dimCampaigns,
      col("marketing_campaign") === col("campaign_code"),
      "left"
    )

  val silverConversionEvents: DataFrame = joinedData.select(
    "country_id",
    "country_name",
    "iso_3_char",
    "iso_2_char",
    "campaign_id",
    "campaign_code",
    "product_group",
    "conversion_event_id",
    "conversion_event",
    "date"
  )

  private def writeDelta(reprocess: Boolean, df: DataFrame) = {
    df.write
      .format("delta")
      .mode(reprocess match {
        case false => "append"
        case _     => "overwrite"
      })
      .save(s"${target}conversion_events")
  }

  private val verificationResult = VerificationSuite()
    .onData(silverConversionEvents)
    .addCheck(
      Check(CheckLevel.Error, "silver layer checks!")
        .isContainedIn(
          "conversion_event",
          Array("Download Content", "Event Registration", "Survey Response"),
          _ >= 0.5
        )
        .isComplete("country_name")
        .hasCompleteness("product_group", _ >= .9)
    )
    .run()

  DeequChecks(verificationResult, spark)
    .runIfSuccess { writeDelta(true, silverConversionEvents) }

}
