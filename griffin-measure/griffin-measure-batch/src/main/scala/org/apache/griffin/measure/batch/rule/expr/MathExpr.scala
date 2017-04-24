package org.apache.griffin.measure.batch.rule.expr

trait MathExpr extends Expr with Calculatable {

}

case class MathFactorExpr(self: Expr with Calculatable) extends MathExpr {
  def calculate(values: Map[String, Any]): Option[Any] = self.calculate(values)
  val desc: String = self.desc
}

case class UnaryMathExpr(oprList: Iterable[String], factor: MathExpr) extends MathExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = oprList.foldRight(factor.desc) { (prev, ex) => s"${prev}${ex}" }
}

case class BinaryMathExpr(first: MathExpr, others: Iterable[(String, MathExpr)]) extends MathExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = others.foldLeft(first.desc) { (ex, next) => s"${ex} ${next._1} ${next._2.desc}" }
}