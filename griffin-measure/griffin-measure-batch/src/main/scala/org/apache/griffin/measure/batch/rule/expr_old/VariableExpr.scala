package org.apache.griffin.measure.batch.rule.expr_old

trait VariableExpr extends Expr with Recordable {

  val name: String

}


case class VariableStringExpr(expression: String) extends VariableExpr {

  val name = expression

  val recordName = name

}

case class QuoteVariableExpr(expression: String) extends VariableExpr with Calculatable {

  val name = expression

  val recordName = name

  def genValue(values: Map[String, Any]): Option[Any] = values.get(name)

}