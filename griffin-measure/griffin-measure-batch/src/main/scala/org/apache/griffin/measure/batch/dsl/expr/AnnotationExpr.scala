package org.apache.griffin.measure.batch.dsl.expr

case class AnnotationExpr(expression: String) extends Expr {

  val value: String = expression

}