package com.packt.dewithscala.chapter1

object HOFInAction extends App {

  def op(x: Int, y: Int)(f: (Int, Int) => Int): Int = f(x, y)

  // function literal
  val add: (Int, Int) => Int = (x: Int, y: Int) => x + y

  // a method
  def multiply(x: Int, y: Int): Int = x * y

  println(op(1, 2)(add))

  println(op(2, 3)(multiply))
}
