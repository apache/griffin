package org.apache.griffin.measure.batch.rule.expr


trait GroupbyableExpr extends Serializable {
  def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = Nil
}