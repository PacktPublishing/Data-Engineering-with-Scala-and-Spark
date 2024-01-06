package com.packt.dewithscala.chapter1

class Point(val x: Int, val y: Int) {

  def add(that: Point): Point = new Point(x + that.x, y + that.y)

  def compare(that: Point) = (x ^ 2 + y ^ 2) ^ 1 / 2 - (that.x ^ 2 +
    that.y ^ 2) ^ 1 / 2

  override def toString: String = s"($x, $y)"
}

object Point {
  def apply(x: Int, y: Int) = new Point(x, y)
}
