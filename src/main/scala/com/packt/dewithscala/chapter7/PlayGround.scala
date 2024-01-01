package com.packt.dewithscala.chapter7

import com.packt.dewithscala.utils._

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.suggestions._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object PlayGround extends App {

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

  // analysis metrics
  val analysisResult: AnalyzerContext = AnalysisRunner
    .onData(df)
    .addAnalyzer(Size())
    .addAnalyzer(Completeness("flight_number"))
    .addAnalyzer(ApproxCountDistinct("airline"))
    .addAnalyzer(Maximum("number_of_flights"))
    .addAnalyzer(Correlation("distance", "arrival_delay"))
    .run()

  val metrics: DataFrame = successMetricsAsDataFrame(session, analysisResult)

  metrics.show(false)

  // constraint suggestions

  val suggestionsResult: ConstraintSuggestionResult =
    ConstraintSuggestionRunner()
      .onData(df)
      .addConstraintRules(Rules.DEFAULT)
      .run

  suggestionsResult.constraintSuggestions.foreach {
    case (column, suggestions) =>
      suggestions.foreach { suggestion =>
        println(
          s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
            s"The corresponding scala code is ${suggestion.codeForConstraint}\n\n"
        )
      }
  }

}
