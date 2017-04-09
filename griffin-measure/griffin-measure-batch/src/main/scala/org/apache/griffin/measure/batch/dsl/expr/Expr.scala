package org.apache.griffin.measure.batch.dsl.expr

trait Expr extends Serializable {

  val expression: String

  val value: Option[Any]

  def entity(values: Map[String, Any]): Expr

}
