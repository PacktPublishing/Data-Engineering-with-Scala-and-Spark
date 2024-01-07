package com.packt.dewithscala.chapter1

object ForComprehensionExample extends App {

  final case class Person(
      firstName: String,
      isFemale: Boolean,
      children: Person*
  )

  private val bob = Person("Bob", false)
  private val jennette = Person("Jennette", true)
  private val laura = Person("Laura", true)
  private val jean = Person("Jean", true, bob, laura)
  private val persons = List(bob, jennette, laura, jean)

  private val mothersAndChildrens = for {
    p <- persons
    if p.isFemale
    c <- p.children
  } yield (p.firstName, c.firstName)

  mothersAndChildrens.foreach(println(_))

}
