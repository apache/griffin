package org.apache.griffin.measure.batch.dsl.expr

trait SelectExpr extends Expr {

  def entity(values: Map[String, Any]): SelectExpr

}


case class NumPositionExpr(expression: String) extends SelectExpr {

  val value: Option[Int] = Some(expression.toInt)

  def entity(values: Map[String, Any]): NumPositionExpr = NumPositionExpr(expression)

}

case class StringPositionExpr(expression: String) extends SelectExpr {

  val value: Option[String] = Some(expression)

  def entity(values: Map[String, Any]): StringPositionExpr = StringPositionExpr(expression)

}

case class AnyPositionExpr(expression: String) extends SelectExpr {

  val value: Option[String] = Some(expression)

  def entity(values: Map[String, Any]): AnyPositionExpr = AnyPositionExpr(expression)

}

case class FilterOprExpr(expression: String, left: VariableExpr, right: ConstExpr) extends SelectExpr {

  val value: Option[String] = Some(expression) // ??

  def entity(values: Map[String, Any]): FilterOprExpr = FilterOprExpr(expression, left.entity(values), right.entity(values))

}

case class FunctionExpr(expression: String, args: Iterable[ConstExpr]) extends SelectExpr {

  val value: Option[String] = Some(expression)

  def entity(values: Map[String, Any]): FunctionExpr = FunctionExpr(expression, args.map(_.entity(values)))

}