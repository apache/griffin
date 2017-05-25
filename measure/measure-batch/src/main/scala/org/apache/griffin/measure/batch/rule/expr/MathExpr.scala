package org.apache.griffin.measure.batch.rule.expr

import org.apache.griffin.measure.batch.rule.CalculationUtil._

trait MathExpr extends Expr {

}

case class MathFactorExpr(self: Expr) extends MathExpr {
  def calculateOnly(values: Map[String, Any]): Option[Any] = self.calculate(values)
  val desc: String = self.desc
  val dataSources: Set[String] = self.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    self.getCacheExprs(ds)
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    self.getFinalCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    self.getPersistExprs(ds)
  }
}

case class UnaryMathExpr(oprList: Iterable[String], factor: Expr) extends MathExpr {
  private val (posOpr, negOpr) = ("+", "-")
  def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val fv = factor.calculate(values)
    oprList.foldRight(fv) { (opr, v) =>
      opr match {
        case this.posOpr => v
        case this.negOpr => -v
        case _ => None
      }
    }
  }
  val desc: String = oprList.foldRight(factor.desc) { (prev, ex) => s"${prev}${ex}" }
  val dataSources: Set[String] = factor.dataSources
  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    factor.getCacheExprs(ds)
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    factor.getFinalCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    factor.getPersistExprs(ds)
  }
}

case class BinaryMathExpr(first: MathExpr, others: Iterable[(String, MathExpr)]) extends MathExpr {
  private val (addOpr, subOpr, mulOpr, divOpr, modOpr) = ("+", "-", "*", "/", "%")
  def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val fv = first.calculate(values)
    others.foldLeft(fv) { (v, pair) =>
      val (opr, next) = pair
      val nv = next.calculate(values)
      opr match {
        case this.addOpr => v + nv
        case this.subOpr => v - nv
        case this.mulOpr => v * nv
        case this.divOpr => v / nv
        case this.modOpr => v % nv
        case _ => None
      }
    }
  }
  val desc: String = others.foldLeft(first.desc) { (ex, next) => s"${ex} ${next._1} ${next._2.desc}" }
  val dataSources: Set[String] = first.dataSources ++ others.flatMap(_._2.dataSources).toSet
  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    first.getCacheExprs(ds) ++ others.flatMap(_._2.getCacheExprs(ds))
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    first.getFinalCacheExprs(ds) ++ others.flatMap(_._2.getFinalCacheExprs(ds))
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    first.getPersistExprs(ds) ++ others.flatMap(_._2.getPersistExprs(ds))
  }
}