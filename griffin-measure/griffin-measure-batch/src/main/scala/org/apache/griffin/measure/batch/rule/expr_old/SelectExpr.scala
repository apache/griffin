package org.apache.griffin.measure.batch.rule.expr_old

trait SelectExpr extends Expr with Recordable {

}


case class NumPositionExpr(expression: String) extends SelectExpr {

  val index: Int = expression.toInt

  val recordName = s"[${value2RecordString(index)}]"

}

case class StringPositionExpr(expression: String) extends SelectExpr {

  val field: String = expression

  val recordName = s"[${value2RecordString(field)}]"

}

case class AnyPositionExpr(expression: String) extends SelectExpr {

  val recordName = s"[${expression}]"

}

case class FilterOprExpr(expression: String, left: VariableExpr, right: ConstExpr) extends SelectExpr {

  val field: String = left.name
  val value: Any = right.value

  val recordName = {
    s"[${value2RecordString(field)}${expression}${value2RecordString(value)}]"
  }

}

case class FunctionExpr(expression: String, args: Iterable[ConstExpr]) extends SelectExpr {

  val recordName = {
    val argsStr = args.map(value2RecordString(_)).mkString(",")
    s".${expression}(${argsStr})"
  }

}