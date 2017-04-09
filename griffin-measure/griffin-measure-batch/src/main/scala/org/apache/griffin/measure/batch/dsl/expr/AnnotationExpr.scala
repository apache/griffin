package org.apache.griffin.measure.batch.dsl.expr

case class AnnotationExpr(expression: String) extends Expr {

  val value: Option[String] = Some(expression)

  val Key = """^(?i)Key$""".r
  val Name = """^(?i)Name(\w+)$""".r

  def isKey: Boolean = expression.equals(Key)
  def getName: Option[String] = {
    expression match {
      case Name(n) => Some(n)
      case _ => None
    }
  }

  def entity(values: Map[String, Any]): AnnotationExpr = AnnotationExpr(expression)

}