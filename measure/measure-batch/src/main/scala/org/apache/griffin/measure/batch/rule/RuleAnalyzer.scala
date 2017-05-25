package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._

case class RuleAnalyzer(rule: StatementExpr) extends Serializable {

  val constData = ""
  private val SourceData = "source"
  private val TargetData = "target"

  val constCacheExprs: Iterable[Expr] = rule.getCacheExprs(constData)
  private val sourceCacheExprs: Iterable[Expr] = rule.getCacheExprs(SourceData)
  private val targetCacheExprs: Iterable[Expr] = rule.getCacheExprs(TargetData)

  private val sourcePersistExprs: Iterable[Expr] = rule.getPersistExprs(SourceData)
  private val targetPersistExprs: Iterable[Expr] = rule.getPersistExprs(TargetData)

  val constFinalCacheExprs: Iterable[Expr] = rule.getFinalCacheExprs(constData).toSet
  private val sourceFinalCacheExprs: Iterable[Expr] = rule.getFinalCacheExprs(SourceData).toSet ++ sourcePersistExprs.toSet
  val targetFinalCacheExprs: Iterable[Expr] = rule.getFinalCacheExprs(TargetData).toSet ++ targetPersistExprs.toSet

  private val groupbyExprPairs: Seq[(Expr, Expr)] = rule.getGroupbyExprPairs((SourceData, TargetData))
  private val sourceGroupbyExprs: Seq[Expr] = groupbyExprPairs.map(_._1)
  private val targetGroupbyExprs: Seq[Expr] = groupbyExprPairs.map(_._2)

  private val whenClauseExprOpt: Option[LogicalExpr] = rule.getWhenClauseExpr

  val sourceRuleExprs: RuleExprs = RuleExprs(sourceGroupbyExprs, sourceCacheExprs,
    sourceFinalCacheExprs, sourcePersistExprs, whenClauseExprOpt)
  val targetRuleExprs: RuleExprs = RuleExprs(targetGroupbyExprs, targetCacheExprs,
    targetFinalCacheExprs, targetPersistExprs, whenClauseExprOpt)

}


// for a single data source
// groupbyExprs: in accuracy case, these exprs could be groupby exprs
// cacheExprs: the exprs value could be caculated independently, and cached for later use
// finalCacheExprs: the root of cachedExprs, cached for later use
// persistExprs: the expr values should be persisted if any recalculation needed
// whenClauseExprOpt: when clause of rule, to determine if the row of data source is filtered
case class RuleExprs(groupbyExprs: Seq[Expr],
                     cacheExprs: Iterable[Expr],
                     finalCacheExprs: Iterable[Expr],
                     persistExprs: Iterable[Expr],
                     whenClauseExprOpt: Option[LogicalExpr]
                    ) {
  // for example: for a rule "$source.name = $target.name AND $source.age < $target.age + (3 * 4)"
  // in this rule, for the target data source, the targetRuleExprs looks like below
  // groupbyExprs: $target.name
  // cacheExprs: $target.name, $target.age, $target.age + (3 * 4)
  // finalCacheExprs: $target.name, $target.age + (3 * 4)
  // persistExprs: $target.name, $target.age + (3 * 4)
  // whenClauseExprOpt: None
}