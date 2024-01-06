package com.packt.dewithscala.chapter1

import scala.util.Random

object PolymorphicFunctionInAction extends {

  def findFirstIn[A](as: List[A], p: A => Boolean): Option[Int] =
    as.zipWithIndex.collect { case (e, i) if p(e) => i }.headOption

  val ints = Random.shuffle((1 to 20).toList)

  findFirstIn[Int](ints, _ == 5)
}
