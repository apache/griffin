package org.apache.griffin.measure.batch.dsl.calc


trait CalcValue extends Serializable {

  val value: Option[Any]

}