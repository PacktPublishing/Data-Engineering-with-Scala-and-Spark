package com.packt.dewithscala.chapter3

import org.apache.spark.sql._

object FirstSparkApplication extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkPlayground")
    .getOrCreate()

  import spark.implicits._

  private[chapter3] final case class Person(
      personId: String,
      firstName: String,
      lastName: String
  )

  val personDataLocation: String =
    "./src/main/scala/com/packt/dewithscala/chapter3/data/person.csv"
  val personDs: Dataset[Person] = spark.read
    .format("csv")
    .option("header", true)
    .load(personDataLocation)
    .as[Person]

  personDs.show(10)

  val personDf: DataFrame = spark.read
    .format("csv")
    .option("header", true)
    .load(personDataLocation)

  personDf.select($"personId").show(10)
}
