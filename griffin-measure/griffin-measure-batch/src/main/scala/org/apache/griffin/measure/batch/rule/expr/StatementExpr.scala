package org.apache.griffin.measure.batch.rule.expr


trait StatementExpr extends Expr with Calculatable with GroupbyableExpr {
  def valid(values: Map[String, Any]): Boolean = true
}

case class SimpleStatementExpr(expr: LogicalExpr) extends StatementExpr {
  def calculate(values: Map[String, Any]): Option[Any] = expr.calculate(values)
  val desc: String = expr.desc
  val dataSources: Set[String] = expr.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    expr.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    expr.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = expr.getGroupbyExprPairs(dsPair)
}

case class WhenClauseStatementExpr(expr: LogicalExpr, whenExpr: LogicalExpr) extends StatementExpr {
  def calculate(values: Map[String, Any]): Option[Any] = expr.calculate(values)
  val desc: String = s"${expr.desc} when ${whenExpr.desc}"

  override def valid(values: Map[String, Any]): Boolean = {
    whenExpr.calculate(values) match {
      case Some(r: Boolean) => r
      case _ => false
    }
  }

  val dataSources: Set[String] = expr.dataSources ++ whenExpr.dataSources
  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    expr.getCacheExprs(ds) ++ whenExpr.getCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    expr.getPersistExprs(ds) ++ whenExpr.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Iterable[(MathExpr, MathExpr)] = expr.getGroupbyExprPairs(dsPair)
}