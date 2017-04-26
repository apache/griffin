package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._

case class RuleAnalyzer(rule: StatementExpr) extends Serializable {

  val GlobalData = ""
  val SourceData = "source"
  val TargetData = "target"

  val globalCacheExprs: Iterable[Expr] = rule.getCacheExprs(GlobalData)
  val sourceCacheExprs: Iterable[Expr] = rule.getCacheExprs(SourceData)
  val targetCacheExprs: Iterable[Expr] = rule.getCacheExprs(TargetData)

  val sourcePersistExprs: Iterable[Expr] = rule.getPersistExprs(SourceData)
  val targetPersistExprs: Iterable[Expr] = rule.getPersistExprs(TargetData)

  val groupbyExprPairs: Seq[(MathExpr, MathExpr)] = rule.getGroupbyExprPairs((SourceData, TargetData))
  val sourceGroupbyExprs: Seq[MathExpr] = groupbyExprPairs.map(_._1)
  val targetGroupbyExprs: Seq[MathExpr] = groupbyExprPairs.map(_._2)

  val whenClauseExpr: Option[LogicalExpr] = rule.getWhenClauseExpr

}
