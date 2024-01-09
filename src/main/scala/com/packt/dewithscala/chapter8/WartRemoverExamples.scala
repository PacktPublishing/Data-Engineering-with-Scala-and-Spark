package com.packt.dewithscala.chapter8

object WartRemoverExamples extends App {

  // [wartremover:PublicInference] Public member must have an explicit type ascription
  // @SuppressWarnings(Array("org.wartremover.warts.PublicInference"))
  val publicVal = 1

  // [wartremover:Null] null is disabled
  // @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private val aNull = null

  // Option#get is disabled - use Option#fold insteadbloop
  // @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private val optionPartial = Some(1).get

  def isNonNegative(x: Int): Boolean = x match {
    case i if i >= 0 => true
    case _           => false
  }

  // [wartremover:NonUnitStatements] Statements must return Unit
  // @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  isNonNegative(10)

  // [wartremover:Var] var is disabled
  private var x = 1

}
