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
package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.sql.Row

import scala.util.{Success, Try}

object ExprValueUtil {

  // from origin data such as a Row of DataFrame, with existed expr value map, calculate related expression, get the expression value
  // for now, one expr only get one value, not supporting one expr get multiple values
  // params:
  // - originData: the origin data such as a Row of DataFrame
  // - expr: the expression to be calculated
  // - existExprValueMap: existed expression value map, which might be used to get some existed expression value during calculation
  // output: the calculated expression value
  private def calcExprValue(originData: Option[Any], expr: Expr, existExprValueMap: Map[String, Any]): Option[Any] = {
    Try {
      expr match {
        case selection: SelectionExpr => {
          selection.selectors.foldLeft(originData) { (dt, selector) =>
            calcExprValue(dt, selector, existExprValueMap)
          }
        }
        case selector: IndexFieldRangeSelectExpr => {
          originData match {
            case Some(row: Row) => {
              if (selector.fields.size == 1) {
                selector.fields.head match {
                  case i: IndexDesc => Some(row.getAs[Any](i.index))
                  case f: FieldDesc => Some(row.getAs[Any](f.field))
                  case _ => None
                }
              } else None
            }
            case _ => None
          }
        }
        case _ => expr.calculate(existExprValueMap)
      }
    } match {
      case Success(v) => v
      case _ => None
    }
  }

  // try to calculate expr from data and initExprValueMap, generate a new expression value map
  // depends on origin data and existed expr value map
  def genExprValueMap(data: Option[Any], expr: Expr, initExprValueMap: Map[String, Any]): Map[String, Any] = {
    val valueOpt = calcExprValue(data, expr, initExprValueMap)
    if (valueOpt.nonEmpty) {
      initExprValueMap + (expr._id -> valueOpt.get)
    } else initExprValueMap
  }

  // try to calculate some exprs from data and initExprValueMap, generate a new expression value map
  // depends on origin data and existed expr value map
  def genExprValueMap(data: Option[Any], exprs: Iterable[Expr], initExprValueMap: Map[String, Any]): Map[String, Any] = {
    exprs.foldLeft(initExprValueMap) { (evMap, expr) =>
      ExprValueUtil.genExprValueMap(None, expr, evMap)
    }
  }

  // with exprValueMap, calculate expressions, update the expression value map
  // only depends on existed expr value map, only calculation, not need origin data
  def updateExprValueMap(exprs: Iterable[Expr], exprValueMap: Map[String, Any]): Map[String, Any] = {
    exprs.foldLeft(Map[String, Any]()) { (evMap, expr) =>
      val valueOpt = expr.calculate(exprValueMap)
      if (valueOpt.nonEmpty) {
        evMap + (expr._id -> valueOpt.get)
      } else evMap
    }
  }

}
