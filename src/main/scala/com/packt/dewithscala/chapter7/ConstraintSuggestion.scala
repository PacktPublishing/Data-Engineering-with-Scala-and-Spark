package com.packt.dewithscala.chapter7

import com.packt.dewithscala.utils._

import com.amazon.deequ.suggestions.ConstraintSuggestionResult
import com.amazon.deequ.suggestions.ConstraintSuggestionRunner
import com.amazon.deequ.suggestions.Rules

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ConstraintSuggestion extends App {

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
            s"The corresponding scala code is ${suggestion.codeForConstraint}"
        )
      }
  }

}
