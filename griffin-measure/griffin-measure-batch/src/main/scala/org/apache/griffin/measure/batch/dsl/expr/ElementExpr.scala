package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

trait ElementExpr extends Expr with ExprAnalyzable with Calculatable {

  def genValue(values: Map[String, Any]): ElementValue

}


// self: const | selection | calculation
case class FactorExpr(self: Expr with Calculatable) extends ElementExpr {

  val expression: String = self.expression

  def genValue(values: Map[String, Any]): FactorValue = FactorValue(self.genValue(values))

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    self match {
      case expr: DataExpr => expr :: Nil
      case expr: CalculationExpr => expr.getDataRelatedExprs(dataSign)
      case _ => Nil
    }
  }

}

case class CalculationExpr(first: ElementExpr, others: Iterable[(String, ElementExpr)]) extends ElementExpr {

  val expression: String = others.foldLeft(first.expression) { (ex, next) => s"${ex} ${next._1} ${next._2}" }

  def genValue(values: Map[String, Any]): CalculationValue = CalculationValue(first.genValue(values), others.map(n => (n._1, n._2.genValue(values))))

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    others.foldLeft(first.getDataRelatedExprs(dataSign)) { (origin, next) =>
      origin ++ next._2.getDataRelatedExprs(dataSign)
    }
  }

}