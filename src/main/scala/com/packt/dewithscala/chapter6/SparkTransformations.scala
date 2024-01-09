package com.packt.dewithscala.chapter6

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}

import java.sql.Date

import cats.syntax.all._

object SparkTransformations extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("dewithscala")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", 1)
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "Legacy")

  import spark.implicits._

  final case class NetflixTitle(
      show_id: String,
      stype: String,
      title: String,
      director: String,
      cast: String,
      country: String,
      date_added: Date,
      release_year: Int,
      rating: String,
      duration: String,
      listed_in: String,
      description: String
  )

  val df: DataFrame = spark.read
    .option("header", true)
    .csv(
      "src/main/scala/com/packt/dewithscala/chapter6/data/netflix_titles.csv"
    )
    .na
    .fill("")

  val dsNetflixTitles: Dataset[NetflixTitle] =
    df
      .withColumnRenamed("type", "stype")
      .withColumn("date_added", to_date($"date_added", "MMMM dd, yyyy"))
      .withColumn("release_year", $"release_year".cast(IntegerType))
      .as[NetflixTitle]

  dsNetflixTitles.show(5)

  dsNetflixTitles.printSchema()

  /** Selcting
    */
  val dfCastMembersSelect: DataFrame = dsNetflixTitles.select(
    $"show_id",
    explode(split($"cast", ",")).alias("cast_member")
  )
  dfCastMembersSelect.show(10)

  val dfCastMembersSelectExpr: DataFrame = dsNetflixTitles
    .selectExpr(
      "show_id",
      "explode(split(cast, ',')) as cast_member"
    )
    .selectExpr("show_id", "trim(cast_member) as cast_member")
  dfCastMembersSelectExpr.show(10)

  val dfDirectorByShowSelectExpr: DataFrame = dsNetflixTitles
    .selectExpr(
      "show_id",
      "explode(split(director, ',')) as director"
    )
    .selectExpr("show_id", "trim(director) as director")
  dfDirectorByShowSelectExpr.show(10)

  final case class CastMember(show_id: String, cast_member: String)
  final case class CastMembers(show_id: String, cast: Seq[String])
  val dsCastMembers: Dataset[CastMember] = dsNetflixTitles
    .map(r => CastMembers(r.show_id, r.cast.split(",")))
    .flatMap({ c =>
      c.cast.map(cm => CastMember(c.show_id, cm.trim()))
    })

  /** Filtering
    */

  dsCastMembers.show(10, 0)

  dsCastMembers.filter(r => r.show_id === "s2").show()

  dsCastMembers.filter(r => r.cast_member.contains("Eric")).show()
  dfCastMembersSelectExpr.filter("cast_member like '%Eric%'").show()
  dfCastMembersSelectExpr.filter($"cast_member".contains("Eric")).show()

  dsCastMembers
    .filter(r => {
      val id: Int = r.show_id.substring(1, r.show_id.length()).toInt
      id < 2 || id === 5
    })
    .show()

  /** Aggregating
    */

  val numDirectorsByShow: DataFrame =
    dfDirectorByShowSelectExpr
      .groupBy($"show_id")
      .agg(
        count($"director").alias("num_director")
      )
  numDirectorsByShow.show(10, 0)

  val numCastByShow: DataFrame =
    dfCastMembersSelectExpr
      .groupBy($"show_id")
      .agg(
        count($"cast_member").alias("num_cast_member")
      )

  numCastByShow.show(10, 0)

  val dfNetflixTitlesWithNumMetrics: DataFrame = dsNetflixTitles
    .join(numCastByShow, "show_id")
    .join(numDirectorsByShow, "show_id")
  dfNetflixTitlesWithNumMetrics.show(10)

  val dfNetflixUSMetrics: DataFrame =
    dfNetflixTitlesWithNumMetrics
      .filter(trim(lower($"country")) === "united states")
      .groupBy(
        $"rating",
        year($"date_added").alias("year"),
        month($"date_added").alias("month")
      )
      .agg(
        count($"rating")
          .alias("num_shows"),
        avg($"num_director")
          .alias("avg_num_director"),
        avg($"num_cast_member").alias("avg_num_cast_member"),
        max($"num_director").alias("max_num_director"),
        max($"num_cast_member").alias("max_num_cast_member"),
        min($"num_director").alias("min_num_director"),
        min($"num_cast_member").alias("min_num_cast_member")
      )
      .sort($"rating".desc)
  dfNetflixUSMetrics.show(10)

  dfNetflixTitlesWithNumMetrics
    .filter(trim(lower($"country")) === "united states")
    .describe("num_cast_member", "num_director")
    .show()

  dfNetflixTitlesWithNumMetrics.select($"date_added").distinct.show()

  /** Windowing
    */
  private val windowSpecRatingMonth =
    Window.partitionBy("rating", "month").orderBy("year", "month")

  private val dfWindowedLagLead = dfNetflixUSMetrics
    .withColumn(
      "cast_per_director",
      $"avg_num_cast_member" / $"avg_num_director"
    )
    .withColumn(
      "previous_avg_cast_member",
      lag("avg_num_cast_member", 1).over(windowSpecRatingMonth)
    )
    .withColumn(
      "next_avg_cast_member",
      lead("avg_num_cast_member", 1).over(windowSpecRatingMonth)
    )
    .withColumn(
      "diff_prev_curr_num_cast",
      abs($"avg_num_cast_member" - $"previous_avg_cast_member")
    )
    .drop("max_num_director", "min_num_director", "avg_num_director")

  dfWindowedLagLead
    .filter("rating = 'PG' and month = 4")
    .select(
      $"rating",
      $"year",
      $"month",
      $"previous_avg_cast_member".alias("prev"),
      $"avg_num_cast_member".alias("curr"),
      $"next_avg_cast_member".alias("next")
    )
    .show(100)

  private val windowSpecRating =
    Window.partitionBy("rating", "year").orderBy($"avg_num_cast_member".desc)

  private val dfWindowedRank = dfNetflixUSMetrics
    .withColumn("dense_rank", dense_rank().over(windowSpecRating))

  dfWindowedRank
    .filter("rating = 'PG'")
    .select($"rating", $"year", $"avg_num_cast_member", $"dense_rank")
    .show(20, 0)

  /** Complex Data
    */

  private val dfDevicesJson = spark.read.json(
    "src/main/scala/com/packt/dewithscala/chapter6/data/devices.json"
  )

  dfDevicesJson.printSchema()

  dfDevicesJson.show(10)

  dfDevicesJson
    .select($"id", explode($"observations").alias("observation"))
    .groupBy("id")
    .agg(
      max($"observation").alias("max_obs"),
      min($"observation").alias("min_obs"),
      avg($"observation").alias("avg_obs")
    )
    .show(10)

  def convertToXml(a: Array[Double]): String = {
    s"<observations>${a.map(i => s"<observation>${i}</observation>").mkString}</observations>"
  }

  import com.databricks.spark.xml.functions.from_xml
  import com.databricks.spark.xml.schema_of_xml

  private val dfDevicesXml = spark.read
    .option("rowTag", "device")
    .format("xml")
    .load("src/main/scala/com/packt/dewithscala/chapter6/data/devices.xml")

  dfDevicesXml.printSchema()

  private val dfDevicesXMLInJson = spark.read.json(
    "src/main/scala/com/packt/dewithscala/chapter6/data/devicesWithXML.json"
  )

  dfDevicesXMLInJson.show(10, 0)

  dfDevicesXMLInJson.printSchema()

  private val dfXmlFun = dfDevicesXMLInJson.withColumn(
    "parsed",
    from_xml(
      $"xmlObservations",
      schema_of_xml(dfDevicesXMLInJson.select("xmlObservations").as[String])
    )
  )

  dfXmlFun.printSchema()

  dfXmlFun
    .select($"id", explode($"parsed.observation").alias("observation"))
    .groupBy("id")
    .agg(
      max($"observation").alias("max_obs"),
      min($"observation").alias("min_obs"),
      avg($"observation").alias("avg_obs")
    )
    .show(10)

}
