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
package org.apache.griffin.measure.rule.expr

import org.apache.spark.sql.types.DataType
import org.apache.griffin.measure.rule.CalculationUtil._

trait SelectExpr extends Expr {
  def calculateOnly(values: Map[String, Any]): Option[Any] = None
}

case class IndexFieldRangeSelectExpr(fields: Iterable[FieldDescOnly]) extends SelectExpr {
  val desc: String = s"[${fields.map(_.desc).mkString(", ")}]"
  val dataSources: Set[String] = Set.empty[String]
}

case class FunctionOperationExpr(func: String, args: Iterable[MathExpr]) extends SelectExpr {
  val desc: String = s".${func}(${args.map(_.desc).mkString(", ")})"
  val dataSources: Set[String] = args.flatMap(_.dataSources).toSet
  override def getSubCacheExprs(ds: String): Iterable[Expr] = args.flatMap(_.getCacheExprs(ds))
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = args.flatMap(_.getFinalCacheExprs(ds))
  override def getSubPersistExprs(ds: String): Iterable[Expr] = args.flatMap(_.getPersistExprs(ds))
}

case class FilterSelectExpr(field: FieldDesc, compare: String, value: MathExpr) extends SelectExpr {
  val desc: String = s"[${field.desc} ${compare} ${value.desc}]"
  val dataSources: Set[String] = value.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = value.getCacheExprs(ds)
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = value.getFinalCacheExprs(ds)
  override def getSubPersistExprs(ds: String): Iterable[Expr] = value.getPersistExprs(ds)
  private val (eqOpr, neqOpr, btOpr, bteOpr, ltOpr, lteOpr) = ("""==?""".r, """!==?""".r, ">", ">=", "<", "<=")
  override def calculateOnly(values: Map[String, Any]): Option[Any] = {
    val (lv, rv) = (values.get(fieldKey), value.calculate(values))
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
  def fieldKey: String = s"__${field.field}"
}

// -- selection --
case class SelectionExpr(head: SelectionHead, selectors: Iterable[SelectExpr]) extends Expr {
  def calculateOnly(values: Map[String, Any]): Option[Any] = values.get(_id)

  val desc: String = {
    val argsString = selectors.map(_.desc).mkString("")
    s"${head.desc}${argsString}"
  }
  val dataSources: Set[String] = {
    val selectorDataSources = selectors.flatMap(_.dataSources).toSet
    selectorDataSources + head.head
  }

  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    selectors.flatMap(_.getCacheExprs(ds))
  }
  override def getSubFinalCacheExprs(ds: String): Iterable[Expr] = {
    selectors.flatMap(_.getFinalCacheExprs(ds))
  }

  override def persistUnit: Boolean = true
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    selectors.flatMap(_.getPersistExprs(ds))
  }
}