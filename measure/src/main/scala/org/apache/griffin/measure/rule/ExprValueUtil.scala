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

//  private def calcExprValue(originDatas: Seq[Option[Any]], expr: Expr, existExprValueMap: Map[String, Any]): Seq[Option[Any]] = {
//    originDatas.flatMap { originData =>
//      calcExprValue(originData, expr, existExprValueMap)
//    }
//  }

  // from origin data such as a Row of DataFrame, with existed expr value map, calculate related expression, get the expression value
  // for now, one expr only get one value, not supporting one expr get multiple values
  // params:
  // - originData: the origin data such as a Row of DataFrame
  // - expr: the expression to be calculated
  // - existExprValueMap: existed expression value map, which might be used to get some existed expression value during calculation
  // output: the calculated expression values
//  private def calcExprValue(originData: Option[Any], expr: Expr, existExprValueMap: Map[String, Any]): Seq[Option[Any]] = {
//    Try {
//      expr match {
//        case selection: SelectionExpr => {
//          selection.selectors.foldLeft(Seq(originData)) { (datas, selector) =>
//            calcExprValue(datas, selector, existExprValueMap)
//          }
//        }
//        case selector: IndexFieldRangeSelectExpr => {
//          originData match {
//            case Some(row: Row) => {
//              if (selector.fields.size == 1) {
//                selector.fields.head match {
//                  case i: IndexDesc => Seq(Some(row.getAs[Any](i.index)))
//                  case f: FieldDesc => Seq(Some(row.getAs[Any](f.field)))
//                  case _ => Nil
//                }
//              } else Nil
//            }
//            case Some(d: Map[String, Any]) => {
//              selector.fields.foldLeft(Seq[Option[Any]]()) { (results, field) =>
//                results ++ (field match {
//                  case f: FieldDesc => opt2Seq(d.get(f.field))
//                  case a: AllFieldsDesc => d.values.map(Some(_)).toSeq
//                  case _ => Nil
//                })
//              }
//            }
//            case Some(d: Seq[Any]) => {
//              selector.fields.foldLeft(Seq[Option[Any]]()) { (results, field) =>
//                results ++ (field match {
//                  case i: IndexDesc => opt2Seq(try { Some(d(i.index)) } catch { case _ => None })
//                  case a: AllFieldsDesc => d.map(Some(_))
//                  case r: FieldRangeDesc => Nil   // not done
//                  case _ => Nil
//                })
//              }
//            }
//            case _ => Nil
//          }
//        }
//        case selector: FunctionOperationExpr => {
//          val args: Array[Option[Any]] = selector.args.map { arg =>
//            arg.calculate(existExprValueMap)
//          }.toArray
//          originData match {
//            case Some(d: String) => {
//              FunctionUtil.invoke(selector.func, Some(d) +: args)
//            }
//            case _ => Nil
//          }
//        }
//        case _ => Seq(expr.calculate(existExprValueMap))
//      }
//    } match {
//      case Success(v) => v
//      case _ => Nil
//    }
//  }

  private def append(path: List[String], step: String): List[String] = {
    path :+ step
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
                    case i: IndexDesc => Some((append(path, i.desc), Some(row.getAs[Any](i.index))))
                    case f: FieldDesc => Some((append(path, f.desc), Some(row.getAs[Any](f.field))))
                    case _ => None
                  }
                }
              }
              case Some(d: Map[String, Any]) => {
                selector.fields.flatMap { field =>
                  field match {
                    case f: FieldDesc => Some((append(path, f.desc), d.get(f.field)))
                    case a: AllFieldsDesc => {
                      d.map { kv =>
                        val (k, v) = kv
                        (append(path, s"${a.desc}_${k}"), Some(v))
                      }
                    }
                    case _ => None
                  }
                }
              }
              case Some(d: Seq[Any]) => {
                selector.fields.flatMap { field =>
                  field match {
                    case i: IndexDesc => {
                      if (i.index >= 0 && i.index < d.size) {
                        Some((append(path, i.desc), Some(d(i.index))))
                      } else None
                    }
                    case a: AllFieldsDesc => {
                      val dt = d.zipWithIndex
                      dt.map { kv =>
                        val (v, i) = kv
                        (append(path, s"${a.desc}_${i}"), Some(v))
                      }
                    }
                    case r: FieldRangeDesc => {
                      (r.startField, r.endField) match {
                        case (si: IndexDesc, ei: IndexDesc) => {
                          if (si.index >= 0 && ei.index < d.size && si.index <= ei.index) {
                            val dt = d.zipWithIndex
                            dt.filter(kv => (kv._2 >= si.index && kv._2 <= ei.index)).map { kv =>
                              val (v, i) = kv
                              (append(path, s"${r.desc}_${i}"), Some(v))
                            }
                          } else None
                        }
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
    val schemaValues: Map[String, List[(List[String], Any)]] = exprs.map { expr =>
      (expr._id, calcExprValues((Nil, data) :: Nil, expr, existExprValueMap).flatMap { pair =>
        pair._2 match {
          case Some(v) => Some((pair._1, v))
          case _ => None
        }
      })
    }.toMap
    SchemaValueCombineUtil.cartesian(schemaValues)
  }

  // try to calculate expr from data and initExprValueMap, generate new expression value maps
  // depends on origin data and existed expr value map
//  def genExprValueMap(data: Option[Any], expr: Expr, initExprValueMap: Map[String, Any]): Seq[Map[String, Any]] = {
//    val valueOpts = calcExprValues(data, expr, initExprValueMap)
//    valueOpts.map { valueOpt =>
//      if (valueOpt.nonEmpty) {
//        initExprValueMap + (expr._id -> valueOpt.get)
//      } else initExprValueMap
//    }
//  }

  // try to calculate some exprs from data and initExprValueMap, generate a new expression value map
  // depends on origin data and existed expr value map
  def genExprValueMaps(data: Option[Any], exprs: Iterable[Expr], initExprValueMap: Map[String, Any]): List[Map[String, Any]] = {
    val valueMaps = calcExprsValues(data, exprs, initExprValueMap)

    valueMaps.map { valueMap =>
      initExprValueMap ++ valueMap
    }
  }

  // with exprValueMap, calculate expressions, update the expression value map
  // only depends on existed expr value map, only calculation, not need origin data
  def updateExprValueMaps(exprs: Iterable[Expr], exprValueMaps: List[Map[String, Any]]): List[Map[String, Any]] = {
    exprValueMaps.flatMap { exprValueMap =>
      genExprValueMaps(None, exprs, exprValueMap)
    }
  }

//  private def opt2Seq(opt: Option[Any]): Seq[Option[Any]] = {
//    opt match {
//      case Some(v) => Seq(opt)
//      case _ => Nil
//    }
//  }

}
