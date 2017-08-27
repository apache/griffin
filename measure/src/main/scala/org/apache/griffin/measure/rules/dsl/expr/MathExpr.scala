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

trait MathExpr extends Expr {
}

case class MathFactorExpr(factor: Expr, withBracket: Boolean) extends MathExpr {

  addChild(factor)

  def desc: String = if (withBracket) s"(${factor.desc})" else factor.desc
  def coalesceDesc: String = factor.coalesceDesc
}

case class UnaryMathExpr(oprs: Seq[String], factor: MathExpr) extends MathExpr {

  addChild(factor)

  def desc: String = {
    oprs.foldRight(factor.desc) { (opr, fac) =>
      s"(${opr}${fac})"
    }
  }
  def coalesceDesc: String = {
    oprs.foldRight(factor.coalesceDesc) { (opr, fac) =>
      s"(${opr}${fac})"
    }
  }
}

case class BinaryMathExpr(factor: MathExpr, tails: Seq[(String, MathExpr)]) extends MathExpr {

  addChildren(factor +: tails.map(_._2))

  def desc: String = {
    val res = tails.foldLeft(factor.desc) { (fac, tail) =>
      val (opr, expr) = tail
      s"${fac} ${opr} ${expr.desc}"
    }
    if (tails.size <= 0) res else s"${res}"
  }
  def coalesceDesc: String = {
    val res = tails.foldLeft(factor.coalesceDesc) { (fac, tail) =>
      val (opr, expr) = tail
      s"${fac} ${opr} ${expr.coalesceDesc}"
    }
    if (tails.size <= 0) res else s"${res}"
  }
}