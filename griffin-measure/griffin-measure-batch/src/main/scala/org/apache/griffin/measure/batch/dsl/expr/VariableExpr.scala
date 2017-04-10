package org.apache.griffin.measure.batch.dsl.expr

trait VariableExpr extends Expr {

  val name: String

}


case class VariableStringExpr(expression: String) extends VariableExpr {

  val name = expression

}

