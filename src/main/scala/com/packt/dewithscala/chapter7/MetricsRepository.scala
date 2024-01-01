package com.packt.dewithscala.chapter7

import com.packt.dewithscala.utils._

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks._
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.ResultKey

import com.google.common.io.Files

import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.amazon.deequ.VerificationResult

@SuppressWarnings(
  Array(
    "org.wartremover.warts.OptionPartial",
    "org.wartremover.warts.TryPartial",
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Nothing"
  )
)
object MetricsRepository extends App {

  val session: SparkSession = Spark.initSparkSession("de-with-scala")

  val db: Database = Database("my_db")

  val df: DataFrame = db
    .multiPartitionRead(
      session = session,
      dbTable = "my_db.flights",
      partitionCol = "day_of_week",
      upperBound = "7",
      lowerBound = "1",
      10
    )
    .filter(col("airline") === lit("US"))

  val tmpFile: File = new File(Files.createTempDir(), "metrics.json")
  val fileRepo: FileSystemMetricsRepository =
    FileSystemMetricsRepository(session, tmpFile.getAbsolutePath())
  val key: ResultKey =
    ResultKey(System.currentTimeMillis(), Map("tag" -> "metricsRepository"))

  val verificationResult: VerificationResult = VerificationSuite()
    .onData(df)
    .addCheck(
      Check(CheckLevel.Error, "checks on flights data")
        .isComplete("airline")
        .isComplete("flight_number")
        .isContainedIn("cancelled", Array("0", "1"))
        .isNonNegative("distance")
        .isContainedIn("cancellation_reason", Array("A", "B", "C", "D"))
    )
    .useRepository(fileRepo)
    .saveOrAppendResult(key)
    .run()

  // get all of the metrics since last 100 secs
  val metricsAsDF: DataFrame = fileRepo
    .load()
    .after(System.currentTimeMillis() - 100000)
    .getSuccessMetricsAsDataFrame(session)

  metricsAsDF.show()

  // get metrics by the resultKey
  fileRepo
    .loadByKey(key)
    .get
    .metricMap
    .foreach { case (a, b) =>
      println(s"For '${b.instance}' ${b.name} is ${b.value.get}")
    }

  // get metrics by tag
  val metricsJSON: String =
    fileRepo
      .load()
      .withTagValues(Map("tag" -> "metricsRepository"))
      .getSuccessMetricsAsJson()

  println(metricsJSON)

}
