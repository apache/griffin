package org.apache.griffin.measure.batch.rule.expr

trait Calculatable extends Serializable {

  def genValue(values: Map[String, Any]): Option[Any]

}
