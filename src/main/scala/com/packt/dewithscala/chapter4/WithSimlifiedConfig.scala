package com.packt.dewithscala.chapter4

import org.apache.spark.sql.SparkSession

import com.packt.dewithscala.utils.Database

object WithSimlifiedConfig extends App {

  private val session = SparkSession
    .builder()
    .appName("de-with-scala")
    .master("local[*]")
    .getOrCreate()

  private val mysqlDB = Database("my_db")

  private val url      = mysqlDB.jbdcURL
  private val username = mysqlDB.username.value
  private val password = mysqlDB.password.value

  private val airportsDF = session.read
    .format("jdbc")
    .option("url", url)
    .option("user", username)
    .option("password", password)
    .option("dbtable", "airports")
    .load()

  airportsDF.show()

}
