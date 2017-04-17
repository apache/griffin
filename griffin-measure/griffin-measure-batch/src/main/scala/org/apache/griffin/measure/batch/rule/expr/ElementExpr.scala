package org.apache.griffin.measure.batch.rule.expr

import org.apache.griffin.measure.batch.utils.CalculationUtil._

trait ElementExpr extends Expr with ExprAnalyzable with Calculatable {

}


// self: const | selection | calculation
case class FactorExpr(self: Expr with Calculatable) extends ElementExpr {

  val expression: String = self.expression

  def genValue(values: Map[String, Any]): Option[Any] = self.genValue(values)

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    self match {
      case expr: DataExpr => if (expr.head.name == dataSign) expr :: Nil else Nil
      case expr: CalculationExpr => expr.getDataRelatedExprs(dataSign)
      case _ => Nil
    }
  }

}

case class CalculationExpr(first: ElementExpr, others: Iterable[(String, ElementExpr)]) extends ElementExpr {

  val expression: String = others.foldLeft(first.expression) { (ex, next) => s"${ex} ${next._1} ${next._2}" }

  def genValue(values: Map[String, Any]): Option[Any] = {
    others.foldLeft(first.genValue(values)) { (v, next) =>
      val (opr, ele) = next
      val eleValue = ele.genValue(values)
      opr match {
        case "+" => v + eleValue
        case "-" => v - eleValue
        case "*" => v * eleValue
        case "/" => v / eleValue
        case "%" => v % eleValue
        case _ => v
      }
    }
  }

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    others.foldLeft(first.getDataRelatedExprs(dataSign)) { (origin, next) =>
      origin ++ next._2.getDataRelatedExprs(dataSign)
    }
  }

}