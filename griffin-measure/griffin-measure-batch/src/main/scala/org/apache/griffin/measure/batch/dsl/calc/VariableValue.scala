package org.apache.griffin.measure.batch.dsl.calc


trait VariableValue extends CalcValue {

}

case class QuoteVariableValue(value: Option[Any]) extends VariableValue {

}