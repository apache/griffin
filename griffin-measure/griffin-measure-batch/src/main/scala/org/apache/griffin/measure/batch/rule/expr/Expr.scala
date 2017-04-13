package org.apache.griffin.measure.batch.rule.expr

trait Expr extends Serializable {

  val expression: String

  protected val _defaultId: String = ""

  val _id = ExprIdCounter.genId(_defaultId)

}
