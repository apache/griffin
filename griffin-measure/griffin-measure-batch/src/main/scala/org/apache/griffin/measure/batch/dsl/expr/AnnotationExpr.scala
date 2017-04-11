package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

case class AnnotationExpr(expression: String) extends Expr with Calculatable {

//  val value: Option[String] = Some(expression)

  val Key = """^(?i)Key$""".r
  val Name = """^(?i)Name(\w+)$""".r

  def isKey: Boolean = {
    expression match {
      case Key() => true
      case _ => false
    }
  }
  def getName: Option[String] = {
    expression match {
      case Name(n) => Some(n)
      case _ => None
    }
  }

  def genValue(values: Map[String, Any]): AnnotationValue = AnnotationValue(Some(expression))

}