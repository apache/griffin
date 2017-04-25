package org.apache.griffin.measure.batch.rule.expr


trait AnalyzableExpr extends Serializable {
  def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = Nil
  def getWhenClauseExpr(): Option[LogicalExpr] = None
}