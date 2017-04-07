package org.apache.griffin.measure.batch.dsl.expr


trait CalcExpr extends Expr {

}


case class FactorExpr(self: Expr) extends CalcExpr {

  val expression: String = ""
  val value: String = expression

}

case class CalculationExpr(first: CalcExpr, others: Iterable[(String, CalcExpr)]) extends CalcExpr {

  val expression: String = ""
  val value: String = expression

}