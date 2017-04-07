package org.apache.griffin.measure.batch.dsl.expr

trait SelectExpr extends Expr {

}


case class NumPositionExpr(expression: String) extends SelectExpr {

  val value: Int = expression.toInt

}

case class StringPositionExpr(expression: String) extends SelectExpr {

  val value: String = expression

}

case class AnyPositionExpr(expression: String) extends SelectExpr {

  val value: String = expression

}

case class FilterOprExpr(expression: String, left: VariableExpr, right: ConstExpr) extends SelectExpr {

  val value: String = expression

}

case class FunctionExpr(expression: String, args: Iterable[ConstExpr]) extends SelectExpr {

  val value: String = expression

}