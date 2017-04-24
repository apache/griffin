package org.apache.griffin.measure.batch.rule.expr

trait ExprDescOnly extends Describable {

}


case class SelectionHead(expr: String) extends ExprDescOnly {
  private val headRegex = """\$(\w+)""".r
  val head: String = expr match {
    case headRegex(v) => v.toLowerCase
    case _ => expr
  }
  val desc: String = "$" + head
}

case class RangeDesc(elements: Iterable[MathExpr]) extends ExprDescOnly {
  val desc: String = {
    val rangeDesc = elements.map(_.desc).mkString(", ")
    s"(${rangeDesc})"
  }
}
