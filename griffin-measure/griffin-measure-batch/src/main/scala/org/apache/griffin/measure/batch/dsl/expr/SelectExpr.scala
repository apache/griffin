package org.apache.griffin.measure.batch.dsl.expr

trait SelectExpr extends Expr {

}


case class NumPositionExpr(expression: String) extends SelectExpr {

  val index: Int = expression.toInt

}

case class StringPositionExpr(expression: String) extends SelectExpr {

  val field: String = expression

}

case class AnyPositionExpr(expression: String) extends SelectExpr {

}

case class FilterOprExpr(expression: String, left: VariableExpr, right: ConstExpr) extends SelectExpr {

  val field: String = left.name
  val value: Any = right.value

}

case class FunctionExpr(expression: String, args: Iterable[ConstExpr]) extends SelectExpr {

}