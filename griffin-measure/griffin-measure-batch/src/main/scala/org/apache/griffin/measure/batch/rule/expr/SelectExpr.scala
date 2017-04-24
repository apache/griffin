package org.apache.griffin.measure.batch.rule.expr

trait SelectExpr extends Expr {

}

case class IndexFieldRangeSelectExpr(fields: Iterable[FieldDescOnly]) extends SelectExpr {
  val desc: String = s"[${fields.mkString(",")}]"
}

case class FunctionOperationExpr(func: String, args: Iterable[MathExpr]) extends SelectExpr {
  val desc: String = s".${func}(${args.map(_.desc).mkString(",")})"
}

case class FilterSelectExpr(field: FieldDesc, compare: String, value: MathExpr) extends SelectExpr {
  val desc: String = s"[${field.desc}${compare}${value.desc}]"
}

// -- selection --
case class SelectionExpr(head: String, selectors: Iterable[SelectExpr]) extends Expr with Calculatable {
  def calculate(values: Map[String, Any]): Option[Any] = values.get(_id)
  val desc: String = {
    val argsString = selectors.map(_.desc).mkString("")
    s"${head}${argsString}"
  }
}