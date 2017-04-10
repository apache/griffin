package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._

case class QuoteVariableExpr(expression: String) extends Expr with Calculatable {

  def genValue(values: Map[String, Any]): QuoteVariableValue = {
    QuoteVariableValue(None)    // fixme: not done
  }

}
