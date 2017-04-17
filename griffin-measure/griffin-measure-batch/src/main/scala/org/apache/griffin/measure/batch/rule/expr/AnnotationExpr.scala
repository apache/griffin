package org.apache.griffin.measure.batch.rule.expr

case class AnnotationExpr(expression: String) extends Expr with Calculatable {

  val Key = """^(?i)Key$""".r

  def isKey: Boolean = {
    expression match {
      case Key() => true
      case _ => false
    }
  }

  def genValue(values: Map[String, Any]): Option[String] = Some(expression)

}