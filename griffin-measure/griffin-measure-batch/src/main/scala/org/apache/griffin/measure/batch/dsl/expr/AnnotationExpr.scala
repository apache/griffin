package org.apache.griffin.measure.batch.dsl.expr

case class AnnotationExpr(expression: String) extends Expr {

  val value: String = expression

  val Key = """^(?i)Key$""".r
  val Name = """^(?i)Name(\w+)$""".r

  def isKey: Boolean = value.equals(Key)
  def getName: Option[String] = {
    value match {
      case Name(n) => Some(n)
      case _ => None
    }
  }

}