package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._

case class RuleAnalyzer(rule: StatementExpr) extends Serializable {

  val constData = ""
  val SourceData = "source"
  val TargetData = "target"

  val constCacheExprs: Iterable[Expr] = rule.getCacheExprs(constData)
  val sourceCacheExprs: Iterable[Expr] = rule.getCacheExprs(SourceData)
  val targetCacheExprs: Iterable[Expr] = rule.getCacheExprs(TargetData)

  val sourcePersistExprs: Iterable[Expr] = rule.getPersistExprs(SourceData)
  val targetPersistExprs: Iterable[Expr] = rule.getPersistExprs(TargetData)

  val constFinalCacheExprs: Iterable[Expr] = rule.getFinalCacheExprs(constData).toSet
  val sourceFinalCacheExprs: Iterable[Expr] = rule.getFinalCacheExprs(SourceData).toSet ++ sourcePersistExprs.toSet
  val targetFinalCacheExprs: Iterable[Expr] = rule.getFinalCacheExprs(TargetData).toSet ++ targetPersistExprs.toSet

  val groupbyExprPairs: Seq[(Expr, Expr)] = rule.getGroupbyExprPairs((SourceData, TargetData))
  val sourceGroupbyExprs: Seq[Expr] = groupbyExprPairs.map(_._1)
  val targetGroupbyExprs: Seq[Expr] = groupbyExprPairs.map(_._2)

  val whenClauseExprOpt: Option[LogicalExpr] = rule.getWhenClauseExpr

  val sourceRuleExprs: RuleExprs = RuleExprs(sourceGroupbyExprs, sourceCacheExprs,
    sourceFinalCacheExprs, sourcePersistExprs, whenClauseExprOpt)
  val targetRuleExprs: RuleExprs = RuleExprs(targetGroupbyExprs, targetCacheExprs,
    targetFinalCacheExprs, targetPersistExprs, whenClauseExprOpt)

}

case class RuleExprs(groupbyExprs: Seq[Expr],
                     cacheExprs: Iterable[Expr],
                     finalCacheExprs: Iterable[Expr],
                     persistExprs: Iterable[Expr],
                     whenClauseExprOpt: Option[LogicalExpr]
                    ) {

}