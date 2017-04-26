package org.apache.griffin.measure.batch.rule.expr

import org.apache.griffin.measure.batch.utils.CalculationUtil._

trait LogicalExpr extends Expr with AnalyzableExpr {
  override def cacheUnit: Boolean = true
}

case class LogicalCompareExpr(left: MathExpr, compare: String, right: MathExpr) extends LogicalExpr {
  private val (eqOpr, neqOpr, btOpr, bteOpr, ltOpr, lteOpr) = ("""==?""".r, """!==?""".r, ">", ">=", "<", "<=")
  def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val (lv, rv) = (left.calculate(values), right.calculate(values))
    compare match {
      case this.eqOpr() => lv === rv
      case this.neqOpr() => lv =!= rv
      case this.btOpr => lv > rv
      case this.bteOpr => lv >= rv
      case this.ltOpr => lv < rv
      case this.lteOpr => lv <= rv
      case _ => None
    }
  }
  val desc: String = s"${left.desc} ${compare} ${right.desc}"
  val dataSources: Set[String] = left.dataSources ++ right.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    left.getCacheExprs(ds) ++ right.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    left.getPersistExprs(ds) ++ right.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(MathExpr, MathExpr)] = {
    if (compare == "=" || compare == "==") {
      (left.dataSourceOpt, right.dataSourceOpt) match {
        case (Some(dsPair._1), Some(dsPair._2)) => (left, right) :: Nil
        case (Some(dsPair._2), Some(dsPair._1)) => (right, left) :: Nil
        case _ => Nil
      }
    } else Nil
  }
}

case class LogicalRangeExpr(left: MathExpr, rangeOpr: String, range: RangeDesc) extends LogicalExpr {
  private val (inOpr, ninOpr, btwnOpr, nbtwnOpr) = ("""(?i)in""".r, """(?i)not\s+in""".r, """(?i)between""".r, """(?i)not\s+between""".r)
  def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val (lv, rvs) = (left.calculate(values), range.elements.map(_.calculate(values)))
    rangeOpr match {
      case this.inOpr() => lv in rvs
      case this.ninOpr() => lv not_in rvs
      case this.btwnOpr() => lv between rvs
      case this.nbtwnOpr() => lv not_between rvs
      case _ => None
    }
  }
  val desc: String = s"${left.desc} ${rangeOpr} ${range.desc}"
  val dataSources: Set[String] = left.dataSources ++ range.elements.flatMap(_.dataSources).toSet
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    left.getCacheExprs(ds) ++ range.elements.flatMap(_.getCacheExprs(ds))
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    left.getPersistExprs(ds) ++ range.elements.flatMap(_.getPersistExprs(ds))
  }
}

// -- logical statement --
//case class LogicalFactorExpr(self: LogicalExpr) extends LogicalExpr {
//  def calculate(values: Map[String, Any]): Option[Any] = self.calculate(values)
//  val desc: String = self.desc
//}

case class UnaryLogicalExpr(oprList: Iterable[String], factor: LogicalExpr) extends LogicalExpr {
  private val notOpr = """(?i)not|!""".r
  def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val fv = factor.calculate(values)
    oprList.foldRight(fv) { (opr, v) =>
      opr match {
        case this.notOpr() => !v
        case _ => None
      }
    }
  }
  val desc: String = oprList.foldRight(factor.desc) { (prev, ex) => s"${prev}${ex}" }
  val dataSources: Set[String] = factor.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    factor.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    factor.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(MathExpr, MathExpr)] = {
    val notOprList = oprList.filter { opr =>
      opr match {
        case this.notOpr() => true
        case _ => false
      }
    }
    if (notOprList.size % 2 == 0) factor.getGroupbyExprPairs(dsPair) else Nil
  }
}

case class BinaryLogicalExpr(first: LogicalExpr, others: Iterable[(String, LogicalExpr)]) extends LogicalExpr {
  private val (andOpr, orOpr) = ("""(?i)and|&&""".r, """(?i)or|\|\|""".r)
  def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val fv = first.calculate(values)
    others.foldLeft(fv) { (v, pair) =>
      val (opr, next) = pair
      val nv = next.calculate(values)
      opr match {
        case this.andOpr() => v && nv
        case this.orOpr() => v || nv
        case _ => None
      }
    }
  }
  val desc: String = others.foldLeft(first.desc) { (ex, next) => s"${ex} ${next._1} ${next._2.desc}" }
  val dataSources: Set[String] = first.dataSources ++ others.flatMap(_._2.dataSources).toSet
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    first.getCacheExprs(ds) ++ others.flatMap(_._2.getCacheExprs(ds))
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    first.getPersistExprs(ds) ++ others.flatMap(_._2.getPersistExprs(ds))
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(MathExpr, MathExpr)] = {
    if (others.isEmpty) first.getGroupbyExprPairs(dsPair)
    else {
      val isAnd = others.exists(_._1 match {
        case this.andOpr() => true
        case _ => false
      })
      if (isAnd) {
        first.getGroupbyExprPairs(dsPair) ++ others.flatMap(_._2.getGroupbyExprPairs(dsPair))
      } else Nil
    }
  }
}