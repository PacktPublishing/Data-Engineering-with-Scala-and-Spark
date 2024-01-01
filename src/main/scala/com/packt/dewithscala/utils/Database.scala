package com.packt.dewithscala.utils

import com.packt.dewithscala.Config._
import com.packt.dewithscala.Opaque

import doobie._
import doobie.implicits._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.apache.spark.sql._

sealed trait Database {
  def driver: String
  def scheme: String
  def host: String
  def port: String
  def name: String
  def jbdcURL: String
  def username: Opaque
  def password: Opaque
  def records[T: Read](selectStatement: String): List[T]
  def runDDL(statement: String): Database
  def runDML(statement: String): Database = runDDL(statement)
  def singlePartitionRead(session: SparkSession, dbTable: String): DataFrame
  def multiPartitionRead(
      session: SparkSession,
      dbTable: String,
      partitionCol: String,
      upperBound: String,
      lowerBound: String,
      numPartitions: Int
  ): DataFrame

  def singlePartitionWrite(
      session: SparkSession,
      dbTable: String,
      df: DataFrame,
      saveMode: SaveMode
  ): Database

  def multiPartitionWrite(
      session: SparkSession,
      dbTable: String,
      df: DataFrame,
      numPartitions: Int,
      saveMode: SaveMode
  ): Database

}

object Database {

  def apply(name: String): Database = new DatabaseImplementation(name)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.OptionPartial",
      "org.wartremover.warts.NonUnitStatements"
    )
  )
  final private class DatabaseImplementation(dbname: String) extends Database {

    private val db = getDB(dbname).get

    def driver = db.driver
    def scheme = db.scheme
    def host   = db.host
    def port   = db.port
    def name   = db.name

    def jbdcURL  = s"$scheme://$host:$port/$name"
    def username = db.username
    def password = db.password

    val transactor = Transactor.fromDriverManager[IO](
      driver,
      jbdcURL,
      username.value,
      password.value
    )

    def records[T: Read](selectStatement: String): List[T] =
      Fragment
        .const(selectStatement)
        .query[T]
        .to[List]
        .transact(transactor)
        .unsafeRunSync()

    def runDDL(statement: String): Database = {
      Fragment
        .const(statement)
        .update
        .run
        .transact(transactor)
        .unsafeRunSync()

      this
    }

    def singlePartitionRead(session: SparkSession, dbTable: String): DataFrame =
      session.read
        .format("jdbc")
        .option("url", jbdcURL)
        .option("user", username.value)
        .option("password", password.value)
        .option("dbtable", dbTable)
        .load()

    def multiPartitionRead(
        session: SparkSession,
        dbTable: String,
        partitionCol: String,
        upperBound: String,
        lowerBound: String,
        numPartitions: Int
    ) = session.read
      .format("jdbc")
      .option("url", jbdcURL)
      .option("user", username.value)
      .option("password", password.value)
      .option("dbtable", dbTable)
      .option("partitionColumn", partitionCol)
      .option("numPartitions", numPartitions)
      .option("upperBound", upperBound)
      .option("lowerBound", lowerBound)
      .load()

    def singlePartitionWrite(
        session: SparkSession,
        dbTable: String,
        df: DataFrame,
        saveMode: SaveMode
    ) = {
      df.write
        .format("jdbc")
        .option("url", jbdcURL)
        .option("user", username.value)
        .option("password", password.value)
        .option("dbtable", dbTable)
        .mode(saveMode)
        .save()
      this
    }

    def multiPartitionWrite(
        session: SparkSession,
        dbTable: String,
        df: DataFrame,
        numPartitions: Int,
        saveMode: SaveMode
    ) = {

      df.write
        .format("jdbc")
        .option("url", jbdcURL)
        .option("user", username.value)
        .option("password", password.value)
        .option("dbtable", dbTable)
        .option("numPartitions", numPartitions)
        .mode(saveMode)
        .save()

      this
    }

  }
}
