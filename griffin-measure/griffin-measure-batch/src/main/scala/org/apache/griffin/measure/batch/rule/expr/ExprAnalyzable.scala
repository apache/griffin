package org.apache.griffin.measure.batch.rule.expr


trait ExprAnalyzable extends Serializable {

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr]

}
