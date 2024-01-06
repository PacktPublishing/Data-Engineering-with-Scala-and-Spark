package com.packt.dewithscala.chapter1

object ForComprehensionExample extends App {

  case class Person(firstName: String, isFemale: Boolean, children: Person*)

  val bob = Person("Bob", false)
  val jennette = Person("Jennette", true)
  val laura = Person("Laura", true)
  val jean = Person("Jean", true, bob, laura)
  val persons = List(bob, jennette, laura, jean)

  for {
    p <- persons
    if p.isFemale
    c <- p.children
  } yield (p.firstName, c.firstName)

}
