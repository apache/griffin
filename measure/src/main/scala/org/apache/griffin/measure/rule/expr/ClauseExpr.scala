/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.rule.expr


trait ClauseExpr extends Expr with AnalyzableExpr {
  def valid(values: Map[String, Any]): Boolean = true
  override def cacheUnit: Boolean = true
}

case class WhereClauseExpr(expr: LogicalExpr) extends ClauseExpr {
  def calculateOnly(values: Map[String, Any]): Option[Any] = expr.calculate(values)
  val desc: String = expr.desc
  val dataSources: Set[String] = expr.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    expr.getCacheExprs(ds)
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    expr.getFinalCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    expr.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(Expr, Expr)] = expr.getGroupbyExprPairs(dsPair)
}

case class WhenClauseExpr(expr: LogicalExpr) extends ClauseExpr {
  def calculateOnly(values: Map[String, Any]): Option[Any] = expr.calculate(values)
  val desc: String = s"WHEN ${expr.desc}"
  val dataSources: Set[String] = expr.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    expr.getCacheExprs(ds)
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    expr.getFinalCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    expr.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(Expr, Expr)] = expr.getGroupbyExprPairs(dsPair)
}

case class StatementExpr(whereClause: WhereClauseExpr, whenClauseOpt: Option[WhenClauseExpr]) extends ClauseExpr {
  def calculateOnly(values: Map[String, Any]): Option[Any] = whereClause.calculate(values)
  val desc: String = {
    whenClauseOpt match {
      case Some(expr) => s"${whereClause.desc} ${expr.desc}"
      case _ => whereClause.desc
    }
  }
  val dataSources: Set[String] = whereClause.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    whereClause.getCacheExprs(ds)
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    whereClause.getFinalCacheExprs(ds)
  }
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    whereClause.getPersistExprs(ds)
  }

  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(Expr, Expr)] = whereClause.getGroupbyExprPairs(dsPair)
}

//case class WhenClauseStatementExpr(expr: LogicalExpr, whenExpr: LogicalExpr) extends ClauseExpr {
//  def calculateOnly(values: Map[String, Any]): Option[Any] = expr.calculate(values)
//  val desc: String = s"${expr.desc} when ${whenExpr.desc}"
//
//  override def valid(values: Map[String, Any]): Boolean = {
//    whenExpr.calculate(values) match {
//      case Some(r: Boolean) => r
//      case _ => false
//    }
//  }
//
//  val dataSources: Set[String] = expr.dataSources ++ whenExpr.dataSources
//  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
//    expr.getCacheExprs(ds) ++ whenExpr.getCacheExprs(ds)
//  }
//  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
//    expr.getFinalCacheExprs(ds) ++ whenExpr.getFinalCacheExprs(ds)
//  }
//  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
//    expr.getPersistExprs(ds) ++ whenExpr.getPersistExprs(ds)
//  }
//
//  override def getGroupbyExprPairs(dsPair: (String, String)): Seq[(Expr, Expr)] = {
//    expr.getGroupbyExprPairs(dsPair) ++ whenExpr.getGroupbyExprPairs(dsPair)
//  }
//  override def getWhenClauseExpr(): Option[LogicalExpr] = Some(whenExpr)
//}