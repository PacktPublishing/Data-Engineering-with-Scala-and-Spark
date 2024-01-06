package com.packt.dewithscala.chapter1

trait List[+A]
case class Cons[+A](head: A, tail: List[A]) extends List[A]
case object Nil extends List[Nothing]
object List {
  def apply[A](as: A*): List[A] =
    if (as.isEmpty) Nil else Cons(as.head, apply(as.tail: _*))
}

object Patterns extends App {

  def threeElements[A](l: List[A]): Boolean = l match {
    case Cons(_, Cons(_, Cons(_, Nil))) => true
    case _                              => false
  }

  println(threeElements(List(true, false)))

  def thirdElement[A](s: Seq[A]): Option[A] = s match {
    case Seq(_, _, a, _*) => Some(a)
    case _                => None
  }

  println(thirdElement(Seq.empty[String]))
}
