package org.apache.griffin.measure.batch.dsl.expr

trait Expr extends Serializable {

  val expression: String

  val _typeString = "expr"
  val _id = ExprIdCounter.genId(_typeString)

}
