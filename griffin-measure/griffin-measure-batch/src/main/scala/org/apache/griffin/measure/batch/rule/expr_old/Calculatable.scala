package org.apache.griffin.measure.batch.rule.expr_old

trait Calculatable extends Serializable {

  def genValue(values: Map[String, Any]): Option[Any]

}
