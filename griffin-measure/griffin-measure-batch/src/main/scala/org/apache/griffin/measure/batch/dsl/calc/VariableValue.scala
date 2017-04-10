package org.apache.griffin.measure.batch.dsl.calc

// get value directly
trait VariableValue extends CalcValue {

}

case class VariableStringValue(value: Option[Any]) extends VariableValue {

}


