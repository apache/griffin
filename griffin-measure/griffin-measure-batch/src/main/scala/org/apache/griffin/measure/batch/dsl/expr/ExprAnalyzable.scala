package org.apache.griffin.measure.batch.dsl.expr


trait ExprAnalyzable extends Serializable {

  def getDataRelatedExprs(dataSign: String): Iterable[DataExpr]

}
