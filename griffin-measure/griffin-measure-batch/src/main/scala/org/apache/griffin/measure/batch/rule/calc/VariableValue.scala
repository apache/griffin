package org.apache.griffin.measure.batch.rule.calc


trait VariableValue extends CalcValue {

}

case class QuoteVariableValue(value: Option[Any]) extends VariableValue {

}