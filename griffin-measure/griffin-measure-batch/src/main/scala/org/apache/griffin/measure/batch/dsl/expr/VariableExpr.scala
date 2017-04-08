package org.apache.griffin.measure.batch.dsl.expr

trait VariableExpr extends Expr {

}


case class VariableStringExpr(expression: String) extends VariableExpr {

  val value: String = expression

}

case class QuoteVariableStringExpr(expression: String) extends VariableExpr {

  val value: String = expression

}