package org.apache.griffin.measure.batch.dsl.expr

import scala.util.Try

trait StatementExpr extends Expr {

}

case class AssignExpr(expression: String, left: VariableExpr, right: CalcExpr) extends StatementExpr {

  val value: String = expression

}

case class ConditionExpr(expression: String, left: CalcExpr, right: CalcExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

  val value: String = expression

}

case class MappingExpr(expression: String, left: CalcExpr, right: CalcExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

  val value: String = expression

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

}

case class StatementsExpr(statements: Iterable[StatementExpr]) extends StatementExpr {

  val expression: String = ""
  val value: String = expression

}