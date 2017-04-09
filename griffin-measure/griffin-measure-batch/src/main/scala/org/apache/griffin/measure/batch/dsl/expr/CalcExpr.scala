package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.utils.CalculationUtil._

trait CalcExpr extends Expr {

  def entity(values: Map[String, Any]): CalcExpr

}


case class FactorExpr(self: Expr) extends CalcExpr {

  val expression: String = self.expression
  val value: Option[Any] = self.value

  def entity(values: Map[String, Any]): FactorExpr = FactorExpr(self.entity(values))

}

case class CalculationExpr(first: CalcExpr, others: Iterable[(String, CalcExpr)]) extends CalcExpr {

  val expression: String = others.foldLeft(first.expression) { (ex, next) => s"${ex} ${next._1} ${next._2}" }
  val value: Option[Any] = others.foldLeft(first.value) { (v, next) =>
    next match {
      case ("+", expr) => v + expr.value
      case ("-", expr) => v - expr.value
      case ("*", expr) => v * expr.value
      case ("/", expr) => v / expr.value
      case ("%", expr) => v % expr.value
      case _ => v
    }
  }

  def entity(values: Map[String, Any]): CalculationExpr = CalculationExpr(first.entity(values), others.map(n => (n._1, n._2.entity(values))))

}