package com.packt.dewithscala.chapter7

import com.packt.dewithscala.utils._

import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataAnalysis extends App {

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

  val analysisResult: AnalyzerContext = AnalysisRunner
    .onData(df)
    .addAnalyzer(Size())
    .addAnalyzer(Completeness("airline"))
    .addAnalyzer(ApproxCountDistinct("origin_airport"))
    .addAnalyzer(Correlation("departure_delay", "arrival_delay"))
    .addAnalyzer(Compliance("no arrival delay", "arrival_delay <= 0"))
    .run()

  successMetricsAsDataFrame(session, analysisResult).show(false)

}
