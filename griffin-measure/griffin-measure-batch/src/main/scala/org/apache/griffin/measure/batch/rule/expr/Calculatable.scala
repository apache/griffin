package org.apache.griffin.measure.batch.rule.expr

import org.apache.griffin.measure.batch.rule.calc._


trait Calculatable extends Serializable {

  def genValue(values: Map[String, Any]): CalcValue

}
