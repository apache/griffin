package org.apache.griffin.measure.batch.dsl.calc

// get value directly
case class AnnotationValue(value: Option[String]) extends CalcValue {

  val Key = """^(?i)Key$""".r
  val Name = """^(?i)Name(\w+)$""".r

  def isKey: Boolean = {
    if (value.nonEmpty) {
      value.get match {
        case Key() => true
        case _ => false
      }
    } else false
  }

  def getName: Option[String] = {
    if (value.nonEmpty) {
      value.get match {
        case Name(n) => Some(n)
        case _ => None
      }
    } else None
  }

}
