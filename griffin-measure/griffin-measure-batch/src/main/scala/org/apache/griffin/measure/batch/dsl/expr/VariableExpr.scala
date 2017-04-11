package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

trait VariableExpr extends Expr {

  val name: String

}


case class VariableStringExpr(expression: String) extends VariableExpr {

  val name = expression

}

case class QuoteVariableExpr(expression: String) extends VariableExpr with Calculatable {

  val name = expression

  def genValue(values: Map[String, Any]): QuoteVariableValue = {
    val value = values.get(name)
    QuoteVariableValue(value)
  }

}