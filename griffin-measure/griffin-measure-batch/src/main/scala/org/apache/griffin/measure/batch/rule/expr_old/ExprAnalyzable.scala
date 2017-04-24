package org.apache.griffin.measure.batch.rule.expr_old


trait ExprAnalyzable extends Serializable {

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr]

}
