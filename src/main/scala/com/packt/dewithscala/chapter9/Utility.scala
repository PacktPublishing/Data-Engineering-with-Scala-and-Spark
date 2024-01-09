package com.packt.dewithscala.chapter9

object Utility extends App {
  def initials(firstName: String, lastName: String): String =
    s"${firstName.substring(0, 1)}${lastName.substring(0, 1)}"
}
