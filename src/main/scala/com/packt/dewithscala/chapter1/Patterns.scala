package com.packt.dewithscala.chapter1

trait CustomList[+A]
final case class Cons[+A](head: A, tail: CustomList[A]) extends CustomList[A]
case object Nil extends CustomList[Nothing]

@SuppressWarnings(
  Array("org.wartremover.warts.IterableOps", "org.wartremover.warts.Recursion")
)
object CustomList {
  def apply[A](as: A*): CustomList[A] =
    if (as.isEmpty) Nil else Cons(as.head, apply(as.drop(1): _*))
}

object Patterns extends App {

  def threeElements[A](l: CustomList[A]): Boolean = l match {
    case Cons(_, Cons(_, Cons(_, Nil))) => true
    case _                              => false
  }

  println(threeElements(CustomList(true, false)))

  def thirdElement[A](s: Seq[A]): Option[A] = s match {
    case Seq(_, _, a, _*) => Some(a)
    case _                => None
  }

  println(thirdElement(Seq.empty[String]))
}
