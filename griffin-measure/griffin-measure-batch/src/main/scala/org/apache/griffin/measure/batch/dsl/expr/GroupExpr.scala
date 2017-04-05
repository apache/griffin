package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.utils.StringParseUtil


case class GroupExpr(expression: String) extends Expr {

  val endLineOpr: String = """[^\];"""

  def getExpressions(): Iterable[String] = {
    StringParseUtil.sepStrings(expression, endLineOpr)
  }

}
