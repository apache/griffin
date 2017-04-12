package org.apache.griffin.measure.batch.rule.calc

// get value directly
trait DataValue extends CalcValue {

}


case class SelectionValue(value: Option[Any]) extends DataValue {

}