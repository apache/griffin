package org.apache.griffin.measure.batch.dsl.expr

trait CalculationExpr extends Expr {

}


case class WholeExpr(head: Expr, tails: Iterable[(String, Expr)]) extends CalculationExpr {

  val expression: String = ""
  val value: String = expression

}