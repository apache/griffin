package org.apache.griffin.measure.batch.rule.expr

trait MathExpr extends Expr {

}

case class MathFactorExpr(self: Expr with Calculatable) extends MathExpr {
  def calculate(values: Map[String, Any]): Option[Any] = self.calculate(values)
  val desc: String = self.desc
  val dataSources: Set[String] = self.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    self.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    self.getPersistExprs(ds)
  }
}

case class UnaryMathExpr(oprList: Iterable[String], factor: MathExpr) extends MathExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = oprList.foldRight(factor.desc) { (prev, ex) => s"${prev}${ex}" }
  val dataSources: Set[String] = factor.dataSources
  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    factor.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    factor.getPersistExprs(ds)
  }
}

case class BinaryMathExpr(first: MathExpr, others: Iterable[(String, MathExpr)]) extends MathExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = others.foldLeft(first.desc) { (ex, next) => s"${ex} ${next._1} ${next._2.desc}" }
  val dataSources: Set[String] = first.dataSources ++ others.flatMap(_._2.dataSources).toSet
  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    first.getCacheExprs(ds) ++ others.flatMap(_._2.getCacheExprs(ds))
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    first.getPersistExprs(ds) ++ others.flatMap(_._2.getPersistExprs(ds))
  }
}