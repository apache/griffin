package org.apache.griffin.measure.batch.rule.expr

trait Expr extends Serializable with Describable {

  protected val _defaultId: String = ""

  val _id = ExprIdCounter.genId(_defaultId)

}
