package com.packt.dewithscala.chapter8

import org.scalatest.funsuite.AnyFunSuite

class IntroToTDDTest extends AnyFunSuite {
  // Tests for add function
  test("Add two positive numbers") {
    assert(IntroToTDD.add(1, 2) == 3)
  }

  test("Add zero to a number") {
    assert(IntroToTDD.add(2, 0) == 2)
  }

  test("Add two negative numbers") {
    assert(IntroToTDD.add(-1, -2) == -3)
  }

  // Tests for multiply function
  test("Multiply two positive numbers") {
    assert(IntroToTDD.multiply(1, 2) == 2)
  }

  test("Multiply a number by zero") {
    assert(IntroToTDD.multiply(2, 0) == 0)
  }

  test("Multiply two negative numbers") {
    assert(IntroToTDD.multiply(-1, -2) == 2)
  }

  test("f(3) results in 30") {
    assert(IntroToTDD.f(3)== 30)
  }

}
