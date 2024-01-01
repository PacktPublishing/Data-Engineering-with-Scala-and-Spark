package com.packt.dewithscala.chapter7

import com.packt.dewithscala.utils.Spark

import com.amazon.deequ._
import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy

import org.apache.spark.sql._

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.Nothing"
  )
)
object AnomalyDetection extends App {

  val session: SparkSession = Spark.initSparkSession("de-with-scala")

  import session.implicits._

  val yesterdayDF: DataFrame =
    Seq((1, "Product 1", 100), (2, "Product 2", 50)).toDF(
      "product_id",
      "product_name",
      "cost_per_unit"
    )

  val repository: InMemoryMetricsRepository = new InMemoryMetricsRepository()

  val yesterdayKey: ResultKey = ResultKey(
    System.currentTimeMillis() - 24 * 60 * 60 * 1000,
    Map("tag" -> "yesterday")
  )

  VerificationSuite()
    .onData(yesterdayDF)
    .useRepository(repository)
    .saveOrAppendResult(yesterdayKey)
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(1.5)),
      Size()
    )
    .run()

  val todayDF: DataFrame = Seq(
    (3, "Product 3", 70),
    (4, "Product 4", 120),
    (5, "Product 5", 65),
    (6, "Product 6", 40)
  ).toDF("product_id", "product_name", "cost_per_unit")

  val todayKey: ResultKey = ResultKey(
    System.currentTimeMillis(),
    Map("tag" -> "now")
  )

  val verificationResult: VerificationResult = VerificationSuite()
    .onData(todayDF)
    .useRepository(repository)
    .saveOrAppendResult(todayKey)
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(1.5)),
      Size()
    )
    .run()

  verificationResult.status match {
    case CheckStatus.Success => println("data looks good")
    case _ =>
      val constraintResults = verificationResult.checkResults.flatMap {
        case (_, checkResult) => checkResult.constraintResults
      }

      constraintResults
        .filter(_.status != ConstraintStatus.Success)
        .foreach { checkResult =>
          println(
            s"for ${checkResult.constraint} the check result was ${checkResult.status}"
          )
          println(checkResult.message.getOrElse(""))

        }
  }

}
