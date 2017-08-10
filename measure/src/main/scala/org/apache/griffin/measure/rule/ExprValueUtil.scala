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
package org.apache.griffin.measure.rule

import org.apache.griffin.measure.rule.expr._
import org.apache.griffin.measure.rule.func._
import org.apache.spark.sql.Row

import scala.util.{Success, Try}

object ExprValueUtil {

  private def append(path: List[String], step: String): List[String] = {
    path :+ step
  }

  private def value2Map(key: String, value: Option[Any]): Map[String, Any] = {
    value.flatMap(v => Some((key -> v))).toMap
  }

  private def getSingleValue(data: Option[Any], desc: FieldDescOnly): Option[Any] = {
    data match {
      case Some(row: Row) => {
        desc match {
          case i: IndexDesc => try { Some(row.getAs[Any](i.index)) } catch { case _ => None }
          case f: FieldDesc => try { Some(row.getAs[Any](f.field)) } catch { case _ => None }
          case _ => None
        }
      }
      case Some(d: Map[String, Any]) => {
        desc match {
          case f: FieldDesc => d.get(f.field)
          case _ => None
        }
      }
      case Some(d: Seq[Any]) => {
        desc match {
          case i: IndexDesc => if (i.index >= 0 && i.index < d.size) Some(d(i.index)) else None
          case _ => None
        }
      }
    }
  }

  private def calcExprValues(pathDatas: List[(List[String], Option[Any])], expr: Expr, existExprValueMap: Map[String, Any]): List[(List[String], Option[Any])] = {
    Try {
      expr match {
        case selection: SelectionExpr => {
          selection.selectors.foldLeft(pathDatas) { (pds, selector) =>
            calcExprValues(pds, selector, existExprValueMap)
          }
        }
        case selector: IndexFieldRangeSelectExpr => {
          pathDatas.flatMap { pathData =>
            val (path, data) = pathData
            data match {
              case Some(row: Row) => {
                selector.fields.flatMap { field =>
                  field match {
                    case (_: IndexDesc) | (_: FieldDesc) => {
                      getSingleValue(data, field).map { v => (append(path, field.desc), Some(v)) }
                    }
                    case a: AllFieldsDesc => {
                      (0 until row.size).flatMap { i =>
                        getSingleValue(data, IndexDesc(i.toString)).map { v =>
                          (append(path, s"${a.desc}_${i}"), Some(v))
                        }
                      }.toList
                    }
                    case r: FieldRangeDesc => {
                      (r.startField, r.endField) match {
                        case (si: IndexDesc, ei: IndexDesc) => {
                          (si.index to ei.index).flatMap { i =>
                            (append(path, s"${r.desc}_${i}"), getSingleValue(data, IndexDesc(i.toString)))
                            getSingleValue(data, IndexDesc(i.toString)).map { v =>
                              (append(path, s"${r.desc}_${i}"), Some(v))
                            }
                          }.toList
                        }
                        case _ => Nil
                      }
                    }
                    case _ => Nil
                  }
                }
              }
              case Some(d: Map[String, Any]) => {
                selector.fields.flatMap { field =>
                  field match {
                    case (_: IndexDesc) | (_: FieldDesc) => {
                      getSingleValue(data, field).map { v => (append(path, field.desc), Some(v)) }
                    }
                    case a: AllFieldsDesc => {
                      d.keySet.flatMap { k =>
                        getSingleValue(data, FieldDesc(k)).map { v =>
                          (append(path, s"${a.desc}_${k}"), Some(v))
                        }
                      }
                    }
                    case _ => None
                  }
                }
              }
              case Some(d: Seq[Any]) => {
                selector.fields.flatMap { field =>
                  field match {
                    case (_: IndexDesc) | (_: FieldDesc) => {
                      getSingleValue(data, field).map { v => (append(path, field.desc), Some(v)) }
                    }
                    case a: AllFieldsDesc => {
                      (0 until d.size).flatMap { i =>
                        (append(path, s"${a.desc}_${i}"), getSingleValue(data, IndexDesc(i.toString)))
                        getSingleValue(data, IndexDesc(i.toString)).map { v =>
                          (append(path, s"${a.desc}_${i}"), Some(v))
                        }
                      }.toList
                    }
                    case r: FieldRangeDesc => {
                      (r.startField, r.endField) match {
                        case (si: IndexDesc, ei: IndexDesc) => {
                          (si.index to ei.index).flatMap { i =>
                            (append(path, s"${r.desc}_${i}"), getSingleValue(data, IndexDesc(i.toString)))
                            getSingleValue(data, IndexDesc(i.toString)).map { v =>
                              (append(path, s"${r.desc}_${i}"), Some(v))
                            }
                          }.toList
                        }
                        case _ => None
                      }
                    }
                    case _ => None
                  }
                }
              }
            }
          }
        }
        case selector: FunctionOperationExpr => {
          val args: Array[Option[Any]] = selector.args.map { arg =>
            arg.calculate(existExprValueMap)
          }.toArray
          pathDatas.flatMap { pathData =>
            val (path, data) = pathData
            data match {
              case Some(d: String) => {
                val res = FunctionUtil.invoke(selector.func, Some(d) +: args)
                val residx = res.zipWithIndex
                residx.map { vi =>
                  val (v, i) = vi
                  val step = if (i == 0) s"${selector.desc}" else s"${selector.desc}_${i}"
                  (append(path, step), v)
                }
              }
              case _ => None
            }
          }
        }
        case selector: FilterSelectExpr => {  // fileter means select the items fit the condition
          pathDatas.flatMap { pathData =>
            val (path, data) = pathData
            data match {
              case Some(row: Row) => {
                // right value could not be selection
                val rmap = value2Map(selector.value._id, selector.value.calculate(existExprValueMap))
                (0 until row.size).flatMap { i =>
                  val dt = getSingleValue(data, IndexDesc(i.toString))
                  val lmap = value2Map(selector.fieldKey, getSingleValue(dt, selector.field))
                  val partValueMap = lmap ++ rmap
                  selector.calculate(partValueMap) match {
                    case Some(true) => Some((append(path, s"${selector.desc}_${i}"), dt))
                    case _ => None
                  }
                }
              }
              case Some(d: Map[String, Any]) => {
                val rmap = value2Map(selector.value._id, selector.value.calculate(existExprValueMap))
                d.keySet.flatMap { k =>
                  val dt = getSingleValue(data, FieldDesc(k))
                  val lmap = value2Map(selector.fieldKey, getSingleValue(dt, selector.field))
                  val partValueMap = lmap ++ rmap
                  selector.calculate(partValueMap) match {
                    case Some(true) => Some((append(path, s"${selector.desc}_${k}"), dt))
                    case _ => None
                  }
                }
              }
              case Some(d: Seq[Any]) => {
                val rmap = value2Map(selector.value._id, selector.value.calculate(existExprValueMap))
                (0 until d.size).flatMap { i =>
                  val dt = getSingleValue(data, IndexDesc(i.toString))
                  val lmap = value2Map(selector.fieldKey, getSingleValue(dt, selector.field))
                  val partValueMap = lmap ++ rmap
                  selector.calculate(partValueMap) match {
                    case Some(true) => Some((append(path, s"${selector.desc}_${i}"), dt))
                    case _ => None
                  }
                }
              }
            }
          }
        }
        case _ => {
          (expr.desc :: Nil, expr.calculate(existExprValueMap)) :: Nil
        }
      }
    } match {
      case Success(v) => v
      case _ => Nil
    }
  }

  private def calcExprsValues(data: Option[Any], exprs: Iterable[Expr], existExprValueMap: Map[String, Any]): List[Map[String, Any]] = {
    val selectionValues: Map[String, List[(List[String], Any)]] = exprs.map { expr =>
      (expr._id, calcExprValues((Nil, data) :: Nil, expr, existExprValueMap).flatMap { pair =>
        pair._2 match {
          case Some(v) => Some((pair._1, v))
          case _ => None
        }
      })
    }.toMap
    // if exprs is empty, return an empty value map for each row
    if (selectionValues.isEmpty) List(Map[String, Any]())
    else SchemaValueCombineUtil.cartesian(selectionValues)
  }

  // try to calculate some exprs from data and initExprValueMap, generate a new expression value map
  // depends on origin data and existed expr value map
  def genExprValueMaps(data: Option[Any], exprs: Iterable[Expr], initExprValueMap: Map[String, Any]): List[Map[String, Any]] = {
    val (selections, nonSelections) = exprs.partition(_.isInstanceOf[SelectionExpr])
    val valueMaps = calcExprsValues(data, selections, initExprValueMap)
    updateExprValueMaps(nonSelections, valueMaps)
  }

  // with exprValueMap, calculate expressions, update the expression value map
  // only depends on existed expr value map, only calculation, not need origin data
  def updateExprValueMaps(exprs: Iterable[Expr], exprValueMaps: List[Map[String, Any]]): List[Map[String, Any]] = {
    exprValueMaps.map { valueMap =>
      exprs.foldLeft(valueMap) { (em, expr) =>
        expr.calculate(em) match {
          case Some(v) => em + (expr._id -> v)
          case _ => em
        }
      }
    }
  }

}
