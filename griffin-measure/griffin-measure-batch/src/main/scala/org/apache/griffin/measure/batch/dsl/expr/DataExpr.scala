package org.apache.griffin.measure.batch.dsl.expr

trait DataExpr extends Expr {

  def entity(values: Map[String, Any]): DataExpr

}


case class SelectionExpr(head: VariableExpr, args: Iterable[SelectExpr]) extends DataExpr {

  val expression: String = ""
  val value: Option[Any] = Some(expression) // ??

  def entity(values: Map[String, Any]): SelectionExpr = SelectionExpr(head.entity(values), args.map(_.entity(values)))

}

