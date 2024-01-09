package com.packt.dewithscala.chapter8

object IntroToTDD extends App {
  def add(a: Integer, b: Integer): Integer = a + b

  def multiply(c: Integer, d: Integer): Integer = c * d

  def f(x: Integer): Integer = {
    multiply(2, (add(3, multiply(4, x))))
  }

  println("The output of f(3) is " + f(3))
}
