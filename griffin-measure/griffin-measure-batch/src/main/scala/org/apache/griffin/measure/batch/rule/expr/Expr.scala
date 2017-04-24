package org.apache.griffin.measure.batch.rule.expr

trait Expr extends Serializable with Describable with Cacheable {

  protected val _defaultId: String = ExprIdCounter.emptyId

  val _id = ExprIdCounter.genId(_defaultId)

}
