package com.packt.dewithscala.chapter9

import org.scalatest.funsuite.AnyFunSuite

class UtilityTest extends AnyFunSuite {
  test("Pass two strings expect correct result") {
    assert(Utility.initials("Eric", "Tome") === "ET")
  }
}
