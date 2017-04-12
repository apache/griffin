package org.apache.griffin.measure.batch.rule.calc

// get value directly
trait ConstValue extends CalcValue {

}


case class ConstStringValue(value: Option[String]) extends ConstValue {

}

case class ConstTimeValue(value: Option[Long]) extends ConstValue {

}

case class ConstNumberValue(value: Option[Long]) extends ConstValue {

}