package com.packt.dewithscala.chapter4

import org.apache.spark.sql.SparkSession

import com.packt.dewithscala.Config
import com.packt.dewithscala._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object ReadTableUsingConfig extends App {

  private val session = SparkSession
    .builder()
    .appName("de-with-scala")
    .master("local[*]")
    .getOrCreate()

  private val mysqlDB = Config.getDB("my_db")

  def getDBParams(param: String): Option[String] = param match {
    case "scheme"   => mysqlDB.map(_.scheme)
    case "host"     => mysqlDB.map(_.host)
    case "port"     => mysqlDB.map(_.port)
    case "name"     => mysqlDB.map(_.name)
    case "username" => mysqlDB.map(_.username.value)
    case "password" => mysqlDB.map(_.password.value)
    case _          => None
  }

  private val scheme   = getDBParams("scheme").get
  private val host     = getDBParams("host").get
  private val port     = getDBParams("port").get
  private val name     = getDBParams("name").get
  private val username = getDBParams("username").get
  private val password = getDBParams("password").get
  private val airportsDF = session.read
    .format("jdbc")
    .option("url", s"$scheme://$host:$port/$name")
    .option("user", username)
    .option("password", password)
    .option("dbtable", "airports")
    .load()

  airportsDF.show()

}
