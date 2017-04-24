package org.apache.griffin.measure.batch.rule.expr


trait LogicalExpr extends Expr with Calculatable {

}

case class LogicalCompareExpr(left: MathExpr, compare: String, right: MathExpr) extends LogicalExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = s"${left.desc} ${compare} ${right.desc}"
  val dataSources: Set[String] = left.dataSources ++ right.dataSources
}

case class LogicalRangeExpr(left: MathExpr, rangeOpr: String, range: RangeDesc) extends LogicalExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = s"${left.desc} ${rangeOpr} ${range.desc}"
  val dataSources: Set[String] = left.dataSources ++ range.dataSources
}

// -- logical statement --
//case class LogicalFactorExpr(self: LogicalExpr) extends LogicalExpr {
//  def calculate(values: Map[String, Any]): Option[Any] = self.calculate(values)
//  val desc: String = self.desc
//}

case class UnaryLogicalExpr(oprList: Iterable[String], factor: LogicalExpr) extends LogicalExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = oprList.foldRight(factor.desc) { (prev, ex) => s"${prev}${ex}" }
  val dataSources: Set[String] = factor.dataSources
}

case class BinaryLogicalExpr(first: LogicalExpr, others: Iterable[(String, LogicalExpr)]) extends LogicalExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = others.foldLeft(first.desc) { (ex, next) => s"${ex} ${next._1} ${next._2.desc}" }
  val dataSources: Set[String] = first.dataSources ++ others.flatMap(_._2.dataSources).toSet
}