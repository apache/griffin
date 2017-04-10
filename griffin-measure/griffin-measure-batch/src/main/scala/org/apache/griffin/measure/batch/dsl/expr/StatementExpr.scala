package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

trait StatementExpr extends Expr with StatementAnalyzable with ExprAnalyzable with Calculatable {

  def genValue(values: Map[String, Any]): StatementValue

}

case class AssignExpr(expression: String, left: VariableExpr, right: ElementExpr) extends StatementExpr {

  def genValue(values: Map[String, Any]): AssignValue = AssignValue(right.genValue(values))

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = right.getDataRelatedExprs(dataSign)

  override def getAssigns(): Iterable[AssignExpr] = this :: Nil

}

case class ConditionExpr(expression: String, left: ElementExpr, right: ElementExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

  def genValue(values: Map[String, Any]): ConditionValue = ConditionValue(expression, left.genValue(values), right.genValue(values), annotations.map(_.genValue(values)))

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    left.getDataRelatedExprs(dataSign) ++ right.getDataRelatedExprs(dataSign)
  }

  override def getConditions(): Iterable[ConditionExpr] = this :: Nil

}

case class MappingExpr(expression: String, left: ElementExpr, right: ElementExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

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

  def genValue(values: Map[String, Any]): MappingValue = MappingValue(expression, left.genValue(values), right.genValue(values), annotations.map(_.genValue(values)))

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    left.getDataRelatedExprs(dataSign) ++ right.getDataRelatedExprs(dataSign)
  }

  override def getMappings(): Iterable[MappingExpr] = this :: Nil
  override def getKeyMappings(): Iterable[MappingExpr] = if (isKey) this :: Nil else Nil

}

case class StatementsExpr(statements: Iterable[StatementExpr]) extends StatementExpr {

  val expression: String = statements.map(_.expression).mkString("\n")

  def genValue(values: Map[String, Any]): StatementsValue = StatementsValue(statements.map(_.genValue(values)))

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    statements.flatMap(_.getDataRelatedExprs(dataSign))
  }

  override def getAssigns(): Iterable[AssignExpr] = {
    statements.flatMap(_.getAssigns())
  }

  override def getConditions(): Iterable[ConditionExpr] = {
    statements.flatMap(_.getConditions())
  }

  override def getMappings(): Iterable[MappingExpr] = {
    statements.flatMap(_.getMappings())
  }

  override def getKeyMappings(): Iterable[MappingExpr] = {
    statements.flatMap(_.getKeyMappings())
  }

}