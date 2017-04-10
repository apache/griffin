package org.apache.griffin.measure.batch.dsl.expr

import org.apache.griffin.measure.batch.dsl.calc._


trait Calculatable extends Serializable {

  def genValue(values: Map[String, Any]): CalcValue

}
