package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._

case class RuleAnalyzer(rule: StatementExpr) extends Serializable {

  val SourceData = "source"
  val TargetData = "target"

  val sourceDataExprs: Iterable[DataExpr] = rule.getDataRelatedExprs(SourceData)
  val targetDataExprs: Iterable[DataExpr] = rule.getDataRelatedExprs(TargetData)

  val keyMappings: Iterable[MappingExpr] = rule.getKeyMappings()

  val sourceDataKeyExprs: Seq[DataExpr] = keyMappings.flatMap(_.getDataRelatedExprs(SourceData)).toSeq
  val targetDatakeyExprs: Seq[DataExpr] = keyMappings.flatMap(_.getDataRelatedExprs(TargetData)).toSeq

  val assigns: Iterable[AssignExpr] = rule.getAssigns()
  val conditions: Iterable[ConditionExpr] = rule.getConditions()
  val mappings: Iterable[MappingExpr] = rule.getMappings()

}
