/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.rule.dsl.expr

trait ClauseExpression extends Expr {
}

case class SelectClause(exprs: Seq[Expr]) extends ClauseExpression {

  addChildren(exprs)

  def desc: String = s"${exprs.map(_.desc).mkString(", ")}"
  def coalesceDesc: String = s"${exprs.map(_.desc).mkString(", ")}"

}

case class WhereClause(expr: Expr) extends ClauseExpression {

  addChild(expr)

  def desc: String = s"WHERE ${expr.desc}"
  def coalesceDesc: String = s"WHERE ${expr.coalesceDesc}"

}

case class GroupbyClause(exprs: Seq[Expr], havingClauseOpt: Option[Expr]) extends ClauseExpression {

  addChildren(exprs ++ havingClauseOpt.toSeq)

  def desc: String = {
    val gbs = exprs.map(_.desc).mkString(", ")
    havingClauseOpt match {
      case Some(having) => s"GROUP BY ${gbs} HAVING ${having.desc}"
      case _ => s"GROUP BY ${gbs}"
    }
  }
  def coalesceDesc: String = {
    val gbs = exprs.map(_.desc).mkString(", ")
    havingClauseOpt match {
      case Some(having) => s"GROUP BY ${gbs} HAVING ${having.coalesceDesc}"
      case _ => s"GROUP BY ${gbs}"
    }
  }

  def merge(other: GroupbyClause): GroupbyClause = {
    val newHavingClauseOpt = (havingClauseOpt, other.havingClauseOpt) match {
      case (Some(hc), Some(ohc)) => {
        val logical1 = LogicalFactorExpr(hc, false, None)
        val logical2 = LogicalFactorExpr(ohc, false, None)
        Some(BinaryLogicalExpr(logical1, ("AND", logical2) :: Nil))
      }
      case (a @ Some(_), _) => a
      case (_, b @ Some(_)) => b
      case (_, _) => None
    }
    GroupbyClause(exprs ++ other.exprs, newHavingClauseOpt)
  }

}

case class OrderbyItem(expr: Expr, orderOpt: Option[String]) extends Expr {
  addChild(expr)
  def desc: String = {
    orderOpt match {
      case Some(os) => s"${expr.desc} ${os.toUpperCase}"
      case _ => s"${expr.desc}"
    }
  }
  def coalesceDesc: String = desc
}

case class OrderbyClause(items: Seq[OrderbyItem]) extends ClauseExpression {

  addChildren(items.map(_.expr))

  def desc: String = {
    val obs = items.map(_.desc).mkString(", ")
    s"ORDER BY ${obs}"
  }
  def coalesceDesc: String = {
    val obs = items.map(_.desc).mkString(", ")
    s"ORDER BY ${obs}"
  }
}

case class LimitClause(expr: Expr) extends ClauseExpression {

  addChild(expr)

  def desc: String = s"LIMIT ${expr.desc}"
  def coalesceDesc: String = s"LIMIT ${expr.coalesceDesc}"
}

case class CombinedClause(selectClause: SelectClause, tails: Seq[ClauseExpression]
                         ) extends ClauseExpression {

  addChildren(selectClause +: tails)

  def desc: String = {
    tails.foldLeft(selectClause.desc) { (head, tail) =>
      s"${head} ${tail.desc}"
    }
  }
  def coalesceDesc: String = {
    tails.foldLeft(selectClause.coalesceDesc) { (head, tail) =>
      s"${head} ${tail.coalesceDesc}"
    }
  }
}

case class ProfilingClause(selectClause: SelectClause, groupbyClauseOpt: Option[GroupbyClause],
                           preGroupbyClauses: Seq[ClauseExpression],
                           postGroupbyClauses: Seq[ClauseExpression]
                          ) extends ClauseExpression {
  addChildren(groupbyClauseOpt match {
    case Some(gc) => (selectClause +: preGroupbyClauses) ++ (gc +: postGroupbyClauses)
    case _ => (selectClause +: preGroupbyClauses) ++ postGroupbyClauses
  })

  def desc: String = {
    val selectDesc = selectClause.desc
    val groupbyDesc = groupbyClauseOpt.map(_.desc).mkString(" ")
    val preDesc = preGroupbyClauses.map(_.desc).mkString(" ")
    val postDesc = postGroupbyClauses.map(_.desc).mkString(" ")
    s"${selectDesc} ${preDesc} ${groupbyDesc} ${postDesc}"
  }
  def coalesceDesc: String = {
    val selectDesc = selectClause.coalesceDesc
    val groupbyDesc = groupbyClauseOpt.map(_.coalesceDesc).mkString(" ")
    val preDesc = preGroupbyClauses.map(_.coalesceDesc).mkString(" ")
    val postDesc = postGroupbyClauses.map(_.coalesceDesc).mkString(" ")
    s"${selectDesc} ${preDesc} ${groupbyDesc} ${postDesc}"
  }
}