package com.packt.dewithscala.utils

import org.apache.spark.sql.SparkSession

object Spark {

  def initSparkSession(appName: String): SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master("local[*]")
    .getOrCreate()
}
