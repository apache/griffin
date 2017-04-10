package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

trait DataExpr extends Expr with Calculatable {

  def head: QuoteVariableExpr
  def args: Iterable[SelectExpr]

  def genValue(values: Map[String, Any]): DataValue

}


case class SelectionExpr(head: QuoteVariableExpr, args: Iterable[SelectExpr]) extends DataExpr {

  val expression: String = ""
  val value: Option[Any] = Some(expression) // fixme: not done

  def genValue(values: Map[String, Any]): SelectionValue = SelectionValue(value)

}

