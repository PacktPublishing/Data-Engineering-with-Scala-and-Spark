package com.packt.dewithscala.chapter12

import com.amazon.deequ._
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.SparkSession

@SuppressWarnings(
  Array("org.wartremover.warts.Equals", "org.wartremover.warts.Throw")
)
final case class DeequChecks(
    verificationResult: VerificationResult,
    session: SparkSession
) {

  def runIfSuccess(body: => Unit): Unit = verificationResult.status match {
    case CheckStatus.Success =>
      body
      VerificationResult
        .successMetricsAsDataFrame(session, verificationResult)
        .show(false)
    case _ =>
      val constraintResults =
        verificationResult.checkResults.flatMap { case (_, checkResult) =>
          checkResult.constraintResults
        }

      val deequValidationError = constraintResults
        .filter(_.status != ConstraintStatus.Success)
        .map { checkResult =>
          s"""|
              |for ${checkResult.constraint} the check result was ${checkResult.status}
              |checkResult.message.getOrElse("")""".stripMargin

        }
        .mkString("\n")

      VerificationResult
        .successMetricsAsDataFrame(session, verificationResult)
        .show(false)

      throw new Exception(deequValidationError)
  }
}
