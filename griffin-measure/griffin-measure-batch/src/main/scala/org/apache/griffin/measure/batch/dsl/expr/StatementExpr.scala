package org.apache.griffin.measure.batch.dsl.expr

trait StatementExpr extends Expr {

}

case class AssignExpr(expression: String, left: Expr, right: Expr) extends SelectExpr {

  val value: String = expression

}

case class ConditionExpr(expression: String, left: Expr, right: Expr, annotations: Iterable[Expr]) extends SelectExpr {

  val value: String = expression

}

case class MappingExpr(expression: String, left: Expr, right: Expr, annotations: Iterable[Expr]) extends SelectExpr {

  val value: String = expression

}

case class StatementsExpr(statements: Iterable[Expr]) extends SelectExpr {

  val expression: String = ""
  val value: String = expression

}