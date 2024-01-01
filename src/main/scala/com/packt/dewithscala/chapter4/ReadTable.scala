package com.packt.dewithscala.chapter4

import org.apache.spark.sql.SparkSession

object ReadTable extends App {
  private[chapter4] val session = SparkSession
    .builder()
    .appName("de-with-scala")
    .master("local[*]")
    .getOrCreate()

  private[chapter4] val airportsDF = session.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/my_db")
    .option("user", "root")
    .option("password", "****") // replace the password
    .option("dbtable", "airports")
    .load()

  airportsDF.show()
}
