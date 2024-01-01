package com.packt.dewithscala.chapter4

import doobie._
import doobie.implicits._

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import com.packt.dewithscala.utils.Database

object WorkingWithDoobie extends App {

  private val db = Database("my_db")

  private val constant: ConnectionIO[String] =
    sql"select 'hello'".query[String].unique

  private val random: ConnectionIO[Double] =
    sql"select rand();".query[Double].unique

  // create a transactor object
  private val transactor = Transactor.fromDriverManager[IO](
    db.driver,
    db.jbdcURL,
    db.username.value,
    db.password.value
  )

  private val run = for {
    c <- constant
    r <- random
  } yield (println(s"($c,$r)"))

  run.transact(transactor).unsafeRunSync()

  final case class Airports(
      iata_code: String,
      airport: String,
      city: String,
      state: String,
      country: String,
      latitude: Double,
      longitude: Double
  )

  private val airports: List[Airports] =
    sql"select * from my_db.airports"
      .query[Airports]      // Query0[Airports]
      .to[List]             // ConnectionIO[List[Airports]]
      .transact(transactor) // IO[List[Airports]]
      .unsafeRunSync()      // List[Airports]

  // print the cities with airports in North Carolina
  airports
    .collect { case a if a.state === "NC" => a.city }
    .foreach(a => println(a))

  private val airports2: List[Airports] =
    db.records[Airports]("select * from my_db.airports")

  airports2
    .collect { case a if a.state === "NC" => a.city }
    .foreach(a => println(a))

  private val airports3 =
    db.records[(String, String, String, String, String, Double, Double)](
      "select * from my_db.airports"
    )

  airports3.collect { case a if a._5 === "NC" => a._3 }.foreach(a => println(a))

}
