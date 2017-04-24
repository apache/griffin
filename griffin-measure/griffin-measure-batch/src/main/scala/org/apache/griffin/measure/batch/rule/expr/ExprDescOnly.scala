package org.apache.griffin.measure.batch.rule.expr

trait ExprDescOnly extends Describable with DataSourceable {

}

case class RangeDesc(elements: Iterable[MathExpr]) extends ExprDescOnly {
  val desc: String = {
    val rangeDesc = elements.map(_.desc).mkString(", ")
    s"(${rangeDesc})"
  }
  val dataSources: Set[String] = elements.flatMap(_.dataSources).toSet
}
