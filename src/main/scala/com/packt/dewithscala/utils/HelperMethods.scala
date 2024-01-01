package com.packt.dewithscala.utils

object HelperMethods {
  def discard[A](f: => A): Unit = {
    val _ = f
    ()
  }
}
