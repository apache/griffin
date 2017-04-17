package org.apache.griffin.measure.batch.rule.expr

trait DataExpr extends Expr with Calculatable with Recordable {

  def head: QuoteVariableExpr
  def args: Iterable[SelectExpr]

}


case class SelectionExpr(head: QuoteVariableExpr, args: Iterable[SelectExpr]) extends {

  val recordName = {
    val argsString = args.map(_.recordName).mkString("")
    s"${head.recordName}${argsString}"
  }

  override protected val _defaultId = recordName

} with DataExpr {

  val expression: String = ""

  def genValue(values: Map[String, Any]): Option[Any] = values.get(_id)

}

