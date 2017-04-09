package org.apache.griffin.measure.batch.dsl.expr

import scala.util.Try

trait StatementExpr extends Expr {

  def entity(values: Map[String, Any]): StatementExpr

}

case class AssignExpr(expression: String, left: VariableExpr, right: CalcExpr) extends StatementExpr {

  val value: Option[Any] = None

  def entity(values: Map[String, Any]): AssignExpr = AssignExpr(expression, left.entity(values), right.entity(values))

}

case class ConditionExpr(expression: String, left: CalcExpr, right: CalcExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

  val value: Option[Boolean] = Some(true) // fixme: need calculation

  def entity(values: Map[String, Any]): ConditionExpr = ConditionExpr(expression, left.entity(values), right.entity(values), annotations.map(_.entity(values)))

}

case class MappingExpr(expression: String, left: CalcExpr, right: CalcExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

  val value: Option[Boolean] = {
    (left.value, right.value) match {
      case (Some(v1), Some(v2)) => Some(v1 == v2)
      case (None, None) => Some(true)
      case _ => Some(false)
    }
  }

  def isKey: Boolean = {
    annotations.exists { e =>
      e match {
        case a: AnnotationExpr => a.isKey
        case _ => false
      }
    }
  }
  def getName: String = {
    val names = annotations.flatMap { e =>
      e match {
        case a: AnnotationExpr => a.getName
        case _ => None
      }
    }
    if (names.size > 0) names.head else {
      "???"   // fixme: need to try to get a name from left expr
    }
  }

  def entity(values: Map[String, Any]): MappingExpr = MappingExpr(expression, left.entity(values), right.entity(values), annotations.map(_.entity(values)))

}

case class StatementsExpr(statements: Iterable[StatementExpr]) extends StatementExpr {

  val expression: String = ""
  val value: Option[Any] = None

  def entity(values: Map[String, Any]): StatementsExpr = StatementsExpr(statements.map(_.entity(values)))

}