package org.apache.griffin.measure.batch.rule.expr

trait ExprDescOnly extends Describable {

}

case class RangeDesc(elements: Iterable[Describable]) extends ExprDescOnly {
  val desc: String = {
    val rangeDesc = elements.map(_.desc).mkString(", ")
    s"(${rangeDesc})"
  }
}
