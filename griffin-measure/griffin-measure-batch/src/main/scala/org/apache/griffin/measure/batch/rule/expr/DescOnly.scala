package org.apache.griffin.measure.batch.rule.expr

import scala.util.{Success, Try}

trait DescOnly extends Describable {

}

case class IndexDesc(expr: String) extends DescOnly {
  val index: Int = {
    Try(expr.toInt) match {
      case Success(v) => v
      case _ => throw new Exception(s"${expr} is invalid index")
    }
  }
  val desc: String = describe(index)
}

case class FieldDesc(expr: String) extends DescOnly {
  val field: String = expr
  val desc: String = describe(field)
}

case class AllFieldsDesc(expr: String) extends DescOnly {
  val allFields: String = expr
  val desc: String = allFields
}

case class FieldRangeDesc(startField: DescOnly, endField: DescOnly) extends DescOnly {
  val desc: String = {
    (startField, endField) match {
      case (f1: IndexDesc, f2: IndexDesc) => s"(${f1.desc}, ${f2.desc})"
      case _ => throw new Exception("invalid field range description")
    }
  }
}

case class RangeDesc(elements: Iterable[Describable]) extends DescOnly {
  val desc: String = {
    val rangeDesc = elements.map(_.desc).mkString(", ")
    s"(${rangeDesc})"
  }
}