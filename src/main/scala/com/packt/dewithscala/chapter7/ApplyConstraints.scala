package com.packt.dewithscala.chapter7

import com.packt.dewithscala.utils._

import com.amazon.deequ.checks._
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.VerificationResult
import com.amazon.deequ.constraints.ConstraintStatus

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@SuppressWarnings(Array("org.wartremover.warts.Equals"))
object ApplyConstraints extends App {

  val session: SparkSession = Spark.initSparkSession("de-with-scala")

  val db: Database = Database("my_db")

  val df: DataFrame = db
    .multiPartitionRead(
      session = session,
      dbTable = "my_db.flights",
      partitionCol = "day_of_week",
      upperBound = "7",
      lowerBound = "1",
      numPartitions = 7
    )
    .filter(col("airline") === lit("US"))

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
