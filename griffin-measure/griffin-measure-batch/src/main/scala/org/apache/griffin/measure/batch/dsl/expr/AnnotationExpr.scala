package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

case class AnnotationExpr(expression: String) extends Expr with Calculatable {

  val Key = """^(?i)Key$""".r

  def isKey: Boolean = {
    expression match {
      case Key() => true
      case _ => false
    }
  }

  def genValue(values: Map[String, Any]): AnnotationValue = AnnotationValue(Some(expression))

}