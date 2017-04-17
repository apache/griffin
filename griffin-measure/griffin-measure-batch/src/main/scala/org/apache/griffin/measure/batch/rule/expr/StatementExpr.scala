package org.apache.griffin.measure.batch.rule.expr

trait StatementExpr extends Expr with StatementAnalyzable with ExprAnalyzable with Calculatable {

}

case class AssignExpr(expression: String, left: VariableExpr, right: ElementExpr) extends StatementExpr {

  def genValue(values: Map[String, Any]): Option[Any] = right.genValue(values)

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = right.getDataRelatedExprs(dataSign)

  override def getAssigns(): Iterable[AssignExpr] = this :: Nil

}

case class ConditionExpr(expression: String, left: ElementExpr, right: ElementExpr, annotations: Iterable[AnnotationExpr]) extends StatementExpr {

  def genValue(values: Map[String, Any]): Option[Boolean] = {
    Some(true) // fixme: not done, need calculation
  }

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

  def genValue(values: Map[String, Any]): Option[Boolean] = {
    (left.genValue(values), right.genValue(values)) match {
      case (Some(v1), Some(v2)) => Some(v1 == v2)
      case (None, None) => Some(true)
      case _ => Some(false)
    }
  }

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = {
    left.getDataRelatedExprs(dataSign) ++ right.getDataRelatedExprs(dataSign)
  }

  override def getMappings(): Iterable[MappingExpr] = this :: Nil
  override def getKeyMappings(): Iterable[MappingExpr] = if (isKey) this :: Nil else Nil

}

case class StatementsExpr(statements: Iterable[StatementExpr]) extends StatementExpr {

  val expression: String = statements.map(_.expression).mkString("\n")

  def genValue(values: Map[String, Any]): Option[Any] = None

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