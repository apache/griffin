package org.apache.griffin.measure.batch.rule.expr

trait Calculatable extends Serializable {

  def calculate(values: Map[String, Any]): Option[Any]

}
