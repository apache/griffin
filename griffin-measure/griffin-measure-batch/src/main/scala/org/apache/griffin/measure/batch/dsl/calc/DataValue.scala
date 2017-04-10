package org.apache.griffin.measure.batch.dsl.calc

// get value directly
trait DataValue extends CalcValue {

}


case class SelectionValue(value: Option[Any]) extends DataValue {

}