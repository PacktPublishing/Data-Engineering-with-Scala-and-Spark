package com.packt.dewithscala.chapter1

abstract class Animal

abstract class Pet extends Animal {
  def name: String
}

class Dog(val name: String) extends Pet {
  override def toString = s"Dog($name)"
}
