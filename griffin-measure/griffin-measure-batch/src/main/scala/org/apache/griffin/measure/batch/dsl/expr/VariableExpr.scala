package org.apache.griffin.measure.batch.dsl.expr

trait VariableExpr extends Expr {

  def entity(values: Map[String, Any]): VariableExpr

}


case class VariableStringExpr(expression: String) extends VariableExpr {

  val value: Option[Any] = Some(expression)   // ??

  def entity(values: Map[String, Any]): VariableStringExpr = VariableStringExpr(expression) // ??

}

case class QuoteVariableStringExpr(expression: String) extends VariableExpr {

  val value: Option[Any] = Some(expression)   // ??

  def entity(values: Map[String, Any]): QuoteVariableStringExpr = QuoteVariableStringExpr(expression) // ??

}