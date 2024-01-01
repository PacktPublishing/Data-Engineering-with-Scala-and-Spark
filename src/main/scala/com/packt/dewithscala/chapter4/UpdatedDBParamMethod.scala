package com.packt.dewithscala.chapter4

import org.apache.spark.sql.SparkSession

import com.packt.dewithscala.Config
import com.packt.dewithscala._

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object UpdatedDBParamMethod extends App {

  private val session = SparkSession
    .builder()
    .appName("de-with-scala")
    .master("local[*]")
    .getOrCreate()

  private val mysqlDB = Config.getDB("my_db").get

  def getDBParams(db: Database): String => Option[String] = param =>
    param match {
      case "scheme"   => Some(db.scheme)
      case "host"     => Some(db.host)
      case "port"     => Some(db.port)
      case "name"     => Some(db.name)
      case "username" => Some(db.username.value)
      case "password" => Some(db.password.value)
      case _          => None
    }

  private val scheme   = getDBParams(mysqlDB)("scheme").get
  private val host     = getDBParams(mysqlDB)("host").get
  private val port     = getDBParams(mysqlDB)("port").get
  private val name     = getDBParams(mysqlDB)("name").get
  private val username = getDBParams(mysqlDB)("username").get
  private val password = getDBParams(mysqlDB)("password").get

  private val airportsDF = session.read
    .format("jdbc")
    .option("url", s"$scheme://$host:$port/$name")
    .option("user", username)
    .option("password", password)
    .option("dbtable", "airports")
    .load()

  airportsDF.show()

}
