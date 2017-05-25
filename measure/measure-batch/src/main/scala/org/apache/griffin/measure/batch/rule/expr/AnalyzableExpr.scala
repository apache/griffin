package org.apache.griffin.measure.batch.rule.expr


trait AnalyzableExpr extends Serializable {
  def getGroupbyExprPairs(dsPair: (String, String)): Seq[(Expr, Expr)] = Nil
  def getWhenClauseExpr(): Option[LogicalExpr] = None
}