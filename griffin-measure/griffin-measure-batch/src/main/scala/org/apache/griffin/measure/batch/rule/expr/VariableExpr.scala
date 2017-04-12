package org.apache.griffin.measure.batch.rule.expr

import org.apache.griffin.measure.batch.rule.calc._

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

  def genValue(values: Map[String, Any]): QuoteVariableValue = {
    val value = values.get(name)
    QuoteVariableValue(value)
  }

}