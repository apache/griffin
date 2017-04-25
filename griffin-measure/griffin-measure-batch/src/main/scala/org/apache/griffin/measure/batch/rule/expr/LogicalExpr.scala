package org.apache.griffin.measure.batch.rule.expr


trait LogicalExpr extends Expr with Calculatable with AnalyzableExpr {
  override def cacheUnit: Boolean = true
}

case class LogicalCompareExpr(left: MathExpr, compare: String, right: MathExpr) extends LogicalExpr {
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = s"${left.desc} ${compare} ${right.desc}"
  val dataSources: Set[String] = left.dataSources ++ right.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    left.getCacheExprs(ds) ++ right.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    left.getPersistExprs(ds) ++ right.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = {
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
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
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
  private val notRegex = """(?i)not|!""".r
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = oprList.foldRight(factor.desc) { (prev, ex) => s"${prev}${ex}" }
  val dataSources: Set[String] = factor.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    factor.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    factor.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = {
    val notOprList = oprList.filter { opr =>
      opr match {
        case notRegex() => true
        case _ => false
      }
    }
    if (notOprList.size % 2 == 0) factor.getGroupbyExprPairs(dsPair) else Nil
  }
}

case class BinaryLogicalExpr(first: LogicalExpr, others: Iterable[(String, LogicalExpr)]) extends LogicalExpr {
  private val andRegex = """(?i)and|&&""".r
  private val orRegex = """(?i)or|\|\|""".r
  def calculate(values: Map[String, Any]): Option[Any] = {
    // fixme
    None
  }
  val desc: String = others.foldLeft(first.desc) { (ex, next) => s"${ex} ${next._1} ${next._2.desc}" }
  val dataSources: Set[String] = first.dataSources ++ others.flatMap(_._2.dataSources).toSet
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    first.getCacheExprs(ds) ++ others.flatMap(_._2.getCacheExprs(ds))
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    first.getPersistExprs(ds) ++ others.flatMap(_._2.getPersistExprs(ds))
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = {
    if (others.isEmpty) first.getGroupbyExprPairs(dsPair)
    else {
      val isAnd = others.exists(_._1 match {
        case andRegex() => true
        case _ => false
      })
      if (isAnd) {
        first.getGroupbyExprPairs(dsPair) ++ others.flatMap(_._2.getGroupbyExprPairs(dsPair))
      } else Nil
    }
  }
}