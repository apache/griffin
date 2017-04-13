package org.apache.griffin.measure.batch.rule.calc


trait CalcValue extends Serializable {

  val value: Option[Any]

}