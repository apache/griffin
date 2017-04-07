package org.apache.griffin.measure.batch.dsl.expr

trait DataExpr extends Expr {

}


case class SelectionExpr(head: VariableExpr, args: Iterable[SelectExpr]) extends DataExpr {

  val expression: String = ""
  val value: String = expression

}

