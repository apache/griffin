package org.apache.griffin.measure.batch.dsl.calc

import org.apache.griffin.measure.batch.utils.CalculationUtil._

trait ElementValue extends CalcValue {

}

// get value from self element
case class FactorValue(self: CalcValue) extends ElementValue {

  val value: Option[Any] = self.value

}

case class CalculationValue(first: ElementValue, others: Iterable[(String, ElementValue)]) extends ElementValue {

  val value: Option[Any] = others.foldLeft(first.value) { (v, next) =>
    next match {
      case ("+", ele) => v + ele.value
      case ("-", ele) => v - ele.value
      case ("*", ele) => v * ele.value
      case ("/", ele) => v / ele.value
      case ("%", ele) => v % ele.value
      case _ => v
    }
  }

}