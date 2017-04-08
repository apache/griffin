package org.apache.griffin.measure.batch.dsl.expr

trait Expr extends Serializable {

  val expression: String

  val value: Any

}
