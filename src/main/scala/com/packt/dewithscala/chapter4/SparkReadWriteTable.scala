package com.packt.dewithscala.chapter4

import com.packt.dewithscala.utils.Database
import com.packt.dewithscala.utils.Spark
import com.packt.dewithscala.utils.HelperMethods

import org.apache.spark.sql.SaveMode

import cats.syntax.all._

object SparkReadWriteTable extends App {

  private val session = Spark.initSparkSession("de-with-scala")

  private val db = Database("my_db")

  final case class Airports(
      iata_code: String,
      airport: String,
      city: String,
      state: String,
      country: String,
      latitude: Double,
      longitude: Double
  )

  private val createTable = """|
                               |create table my_db.flight_count(
                               |  origin_state           varchar(100),
                               |  origin_city            varchar(100),
                               |  origin_airport         varchar(500),
                               |  destination_state      varchar(100),
                               |  destination_city       varchar(100),
                               |  destination_airport    varchar(500),
                               |  number_of_flights      int
                               |);""".stripMargin

  private val insert =
    """|
       |insert into my_db.flight_count
       |select 
       |  oa.state   o_state
       |, oa.city    o_city
       |, oa.airport o_airport
       |, da.state   d_state
       |, da.city    d_city
       |, da.airport d_airport
       |, count(1)
       |from my_db.airlines 
       |inner join my_db.flights on airlines.iata_code = flights.airline
       |inner join my_db.airports oa on flights.origin_airport = oa.iata_code
       |inner join my_db.airports da on flights.destination_airport = da.iata_code
       |group by   
       |  oa.state
       |, oa.city
       |, oa.airport
       |, da.state
       |, da.city
       |, da.airport
       |""".stripMargin

  final case class FlightCount(
      origin_state: String,
      origin_city: String,
      origin_airport: String,
      destination_state: String,
      destination_city: String,
      destination_airport: String,
      number_of_flights: Int
  )

  private val flightCount: List[FlightCount] = db
    .runDDL("drop table if exists my_db.flight_count;")
    .runDDL(createTable)
    .runDML(insert)
    .records[FlightCount]("select * from my_db.flight_count;")

  flightCount
    .collect { case fc if fc.origin_state === fc.destination_state => fc }
    .foreach(a => println(a))

  // single partition read
  db.singlePartitionRead(
    session,
    "(select * from my_db.flight_count order by number_of_flights desc limit 5) qry"
  ).show()

  // to store upper and lower bounds
  final case class MaxMin(max: Int, min: Int)

  // upper and lower bounds
  private val bounds = db
    .records[MaxMin](
      "select max(number_of_flights) max, min(number_of_flights) min from my_db.flight_count"
    )
    .headOption
    .getOrElse(MaxMin(0, 0))

  // multiple partition read
  private val df = db.multiPartitionRead(
    session = session,
    dbTable = "my_db.flight_count",
    partitionCol = "number_of_flights",
    upperBound = bounds.max.toString,
    lowerBound = bounds.min.toString,
    10
  )

  // write df to two new tables
  // one with single partition write
  // another with multi partition write

  HelperMethods.discard {
    db
      .singlePartitionWrite(
        session,
        "my_db.flight_count_2",
        df,
        SaveMode.Overwrite
      )
      .multiPartitionWrite(
        session,
        "my_db.flight_count_3",
        df,
        10,
        SaveMode.Overwrite
      )
  }

}
