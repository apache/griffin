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
package org.apache.griffin.measure.rules.dsl.expr

trait LogicalExpr extends Expr {
}

case class InExpr(head: Expr, is: Boolean, range: Seq[Expr]) extends LogicalExpr {

  addChildren(head +: range)

  def desc: String = {
    val notStr = if (is) "" else " NOT"
    s"${head.desc}${notStr} IN (${range.map(_.desc).mkString(", ")})"
  }
}

case class BetweenExpr(head: Expr, is: Boolean, range: Seq[Expr]) extends LogicalExpr {

  range match {
    case first :: second :: _ => addChildren(head :: first :: second :: Nil)
    case _ => throw new Exception("between expression exception: range less than 2")
  }

  def desc: String = {
    val notStr = if (is) "" else " NOT"
    val rangeStr = range match {
      case first :: second :: _ => s"${first.desc} AND ${second.desc}"
      case _ => throw new Exception("between expression exception: range less than 2")
    }
    s"${head.desc}${notStr} BETWEEN ${rangeStr}"
  }
}

case class LikeExpr(head: Expr, is: Boolean, value: Expr) extends LogicalExpr {

  addChildren(head :: value :: Nil)

  def desc: String = {
    val notStr = if (is) "" else " NOT"
    s"${head.desc}${notStr} LIKE ${value.desc}"
  }
}

case class IsNullExpr(head: Expr, is: Boolean) extends LogicalExpr {

  addChild(head)

  def desc: String = {
    val notStr = if (is) "" else " NOT"
    s"${head.desc} IS${notStr} NULL"
  }
}

case class IsNanExpr(head: Expr, is: Boolean) extends LogicalExpr {

  addChild(head)

  def desc: String = {
    val notStr = if (is) "" else "NOT "
    s"${notStr}isnan(${head.desc})"
  }
}

// -----------

case class LogicalFactorExpr(factor: Expr, withBracket: Boolean) extends LogicalExpr {

  addChild(factor)

  def desc: String = {
    if (withBracket) s"(${factor.desc})" else factor.desc
  }
}

case class UnaryLogicalExpr(oprs: Seq[String], factor: LogicalExpr) extends LogicalExpr {

  addChild(factor)

  def desc: String = {
    oprs.foldRight(factor.desc) { (opr, fac) =>
      s"(${trans(opr)} ${fac})"
    }
  }
  private def trans(s: String): String = {
    s match {
      case "!" => "NOT"
      case _ => s.toUpperCase
    }
  }
}

case class BinaryLogicalExpr(factor: LogicalExpr, tails: Seq[(String, LogicalExpr)]) extends LogicalExpr {

  addChildren(factor +: tails.map(_._2))

  def desc: String = {
    val res = tails.foldLeft(factor.desc) { (fac, tail) =>
      val (opr, expr) = tail
      s"${fac} ${trans(opr)} ${expr.desc}"
    }
    if (tails.size <= 0) res else s"${res}"
  }
  private def trans(s: String): String = {
    s match {
      case "&&" => "AND"
      case "||" => "OR"
      case _ => s.toUpperCase
    }
  }
}