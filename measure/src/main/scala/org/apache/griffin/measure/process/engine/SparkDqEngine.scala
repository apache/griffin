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
package org.apache.griffin.measure.process.engine

import org.apache.griffin.measure.cache.tmst.{TempName, TmstCache}
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.{BatchProcessType, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.adaptor.InternalColumns
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.griffin.measure.utils.ParamUtil._

trait SparkDqEngine extends DqEngine {

  val sqlContext: SQLContext

  val emptyMetricMap = Map[Long, Map[String, Any]]()
  val emptyMap = Map[String, Any]()
  val emptyRecordMap = Map[Long, DataFrame]()

  private def getMetricMaps(dfName: String): Seq[Map[String, Any]] = {
    val pdf = sqlContext.table(s"`${dfName}`")
    val records = pdf.toJSON.collect()
    if (records.size > 0) {
      records.flatMap { rec =>
        try {
          val value = JsonUtil.toAnyMap(rec)
          Some(value)
        } catch {
          case e: Throwable => None
        }
      }.toSeq
    } else Nil
  }

  private def normalizeMetric(metrics: Seq[Map[String, Any]], name: String, collectType: CollectType
                             ): Map[String, Any] = {
    collectType match {
      case EntriesCollectType => metrics.headOption.getOrElse(emptyMap)
      case ArrayCollectType => Map[String, Any]((name -> metrics))
      case MapCollectType => {
        val v = metrics.headOption.getOrElse(emptyMap)
        Map[String, Any]((name -> v))
      }
      case _ => {
        if (metrics.size > 1) Map[String, Any]((name -> metrics))
        else metrics.headOption.getOrElse(emptyMap)
      }
    }
  }

  def collectMetrics(timeInfo: TimeInfo, metricExport: MetricExport, procType: ProcessType
                    ): Map[Long, Map[String, Any]] = {
    if (collectable) {
      val MetricExport(name, stepName, collectType) = metricExport
      try {
        val metricMaps = getMetricMaps(stepName)
        if (metricMaps.size > 0) {
          procType match {
            case BatchProcessType => {
              val metrics: Map[String, Any] = normalizeMetric(metricMaps, name, collectType)
              emptyMetricMap + (timeInfo.calcTime -> metrics)
            }
            case StreamingProcessType => {
              val tmstMetrics = metricMaps.map { metric =>
                val tmst = metric.getLong(InternalColumns.tmst, timeInfo.calcTime)
                val pureMetric = metric.removeKeys(InternalColumns.columns)
                (tmst, pureMetric)
              }
              tmstMetrics.groupBy(_._1).map { pair =>
                val (k, v) = pair
                val maps = v.map(_._2)
                val mtc = normalizeMetric(maps, name, collectType)
                (k, mtc)
              }
            }
          }
        } else {
          info(s"empty metrics of [${name}], not persisted")
          emptyMetricMap
        }
      } catch {
        case e: Throwable => {
          error(s"collect metrics ${name} error: ${e.getMessage}")
          emptyMetricMap
        }
      }
    } else emptyMetricMap
  }


  def collectRecords(timeInfo: TimeInfo, recordExport: RecordExport, procType: ProcessType
                    ): Map[Long, DataFrame] = {
    if (collectable) {
      val RecordExport(_, stepName, _, originDFOpt) = recordExport
      val stepDf = sqlContext.table(s"`${stepName}`")
      val recordsDf = originDFOpt match {
        case Some(originName) => sqlContext.table(s"`${originName}`")
        case _ => stepDf
      }

      procType match {
        case BatchProcessType => {
          val recordsDf = sqlContext.table(s"`${stepName}`")
          emptyRecordMap + (timeInfo.calcTime -> recordsDf)
        }
        case StreamingProcessType => {
          originDFOpt match {
            case Some(originName) => {
              val recordsDf = sqlContext.table(s"`${originName}`")
              stepDf.collect.map { row =>
                val tmst = row.getAs[Long](InternalColumns.tmst)
                val trdf = recordsDf.filter(s"`${InternalColumns.tmst}` = ${tmst}")
                (tmst, trdf)
              }.toMap
            }
            case _ => {
              val recordsDf = sqlContext.table(s"`${stepName}`")
              emptyRecordMap + (timeInfo.calcTime -> recordsDf)
            }
          }
        }
      }
    } else emptyRecordMap
  }

  private def getRecordDataFrame(recordExport: RecordExport): Option[DataFrame] = {
    if (collectable) {
      val RecordExport(_, stepName, _, _) = recordExport
      val stepDf = sqlContext.table(s"`${stepName}`")
      Some(stepDf)
    } else None
  }

  def collectBatchRecords(recordExport: RecordExport): Option[RDD[String]] = {
    getRecordDataFrame(recordExport).map(_.toJSON)
  }

  def collectStreamingRecords(recordExport: RecordExport): (Option[RDD[(Long, Iterable[String])]], Set[Long]) = {
    val RecordExport(_, _, _, originDFOpt) = recordExport
    getRecordDataFrame(recordExport) match {
      case Some(stepDf) => {
        originDFOpt match {
          case Some(originName) => {
            val tmsts = (stepDf.collect.flatMap { row =>
              try {
                val tmst = row.getAs[Long](InternalColumns.tmst)
                val empty = row.getAs[Boolean](InternalColumns.empty)
                Some((tmst, empty))
              } catch {
                case _: Throwable => None
              }
            })
            val emptyTmsts = tmsts.filter(_._2).map(_._1).toSet
            val recordTmsts = tmsts.filter(!_._2).map(_._1).toSet
            if (recordTmsts.size > 0) {
              val recordsDf = sqlContext.table(s"`${originName}`")
              val records = recordsDf.flatMap { row =>
                val tmst = row.getAs[Long](InternalColumns.tmst)
                if (recordTmsts.contains(tmst)) {
                  try {
                    val map = SparkRowFormatter.formatRow(row)
                    val str = JsonUtil.toJson(map)
                    Some((tmst, str))
                  } catch {
                    case e: Throwable => None
                  }
                } else None
              }
              (Some(records.groupByKey), emptyTmsts)
            } else (None, emptyTmsts)
          }
          case _ => {
            val records = stepDf.flatMap { row =>
              val tmst = row.getAs[Long](InternalColumns.tmst)
              try {
                val map = SparkRowFormatter.formatRow(row)
                val str = JsonUtil.toJson(map)
                Some((tmst, str))
              } catch {
                case e: Throwable => None
              }
            }
            (Some(records.groupByKey), Set[Long]())
          }
        }
      }
      case _ => (None, Set[Long]())
    }
//    val recordsOpt = getRecordDataFrame(recordExport).flatMap { stepDf =>
//      originDFOpt match {
//        case Some(originName) => {
//          val tmsts = (stepDf.collect.flatMap { row =>
//            try {
//              val tmst = row.getAs[Long](InternalColumns.tmst)
//              val empty = row.getAs[Boolean](InternalColumns.empty)
//              Some((tmst, empty))
//            } catch {
//              case _: Throwable => None
//            }
//          })
//          val emptyTmsts = tmsts.filter(_._2).map(_._1).toSet
//          val recordTmsts = tmsts.filter(!_._2).map(_._1).toSet
//          if (recordTmsts.size > 0) {
//            val recordsDf = sqlContext.table(s"`${originName}`")
//            val records = recordsDf.flatMap { row =>
//              val tmst = row.getAs[Long](InternalColumns.tmst)
//              if (recordTmsts.contains(tmst)) {
//                try {
//                  val map = SparkRowFormatter.formatRow(row)
//                  val str = JsonUtil.toJson(map)
//                  Some((tmst, str))
//                } catch {
//                  case e: Throwable => None
//                }
//              } else None
//            }
//            Some((Some(records.groupByKey), emptyTmsts))
//          } else Some((None, emptyTmsts))
//        }
//        case _ => {
//          val records = stepDf.flatMap { row =>
//            val tmst = row.getAs[Long](InternalColumns.tmst)
//            try {
//              val map = SparkRowFormatter.formatRow(row)
//              val str = JsonUtil.toJson(map)
//              Some((tmst, str))
//            } catch {
//              case e: Throwable => None
//            }
//          }
//          Some(records.groupByKey)
//        }
//      }
//    }
  }

//
//  def collectUpdateRDD(ruleStep: ConcreteRuleStep): Option[DataFrame] = {
//    if (collectable) {
//      ruleStep match {
//        case step: ConcreteRuleStep if ((step.ruleInfo.persistType == RecordPersistType)
//          || (step.ruleInfo.cacheDataSourceOpt.nonEmpty)) => {
//          val tmst = step.timeInfo.tmst
////          val metricName = step.ruleInfo.name
//
//          step.ruleInfo.tmstNameOpt match {
//            case Some(metricTmstName) => {
//              try {
//                val pdf = sqlContext.table(s"`${metricTmstName}`")
//                Some(pdf)
//              } catch {
//                case e: Throwable => {
//                  error(s"collect records ${metricTmstName} error: ${e.getMessage}")
//                  None
//                }
//              }
//            }
//            case _ => None
//          }
//        }
//        case _ => None
//      }
//    } else None
//  }





//  def collectUpdateRDD(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]
//                      ): Option[RDD[(Long, Iterable[String])]] = {
//    if (collectable) {
//      ruleStep match {
//        case step: ConcreteRuleStep if ((step.ruleInfo.persistType == RecordPersistType)
//          || (step.ruleInfo.cacheDataSourceOpt.nonEmpty)) => {
//          val tmst = step.timeInfo.tmst
//          val metricName = step.ruleInfo.name
//
//          step.ruleInfo.tmstNameOpt match {
//            case Some(metricTmstName) => {
//              try {
//                val pdf = sqlContext.table(s"`${metricTmstName}`")
//                val cols = pdf.columns
//                val rdd = pdf.flatMap { row =>
//                  val values = cols.flatMap { col =>
//                    Some((col, row.getAs[Any](col)))
//                  }.toMap
//                  values.get(GroupByColumn.tmst) match {
//                    case Some(t: Long) if (timeGroups.exists(_ == t)) => Some((t, JsonUtil.toJson(values)))
//                    case _ => None
//                  }
//                }.groupByKey()
//
//                // find other keys in time groups, create empty records for those timestamps
//                val existKeys = rdd.keys.collect
//                val otherKeys = timeGroups.filter(t => !existKeys.exists(_ == t))
//                val otherPairs = otherKeys.map((_, Iterable[String]())).toSeq
//                val otherPairRdd = sqlContext.sparkContext.parallelize(otherPairs)
//
//                Some(rdd union otherPairRdd)
//              } catch {
//                case e: Throwable => {
//                  error(s"collect records ${metricTmstName} error: ${e.getMessage}")
//                  None
//                }
//              }
//            }
//            case _ => None
//          }
//        }
//        case _ => None
//      }
//    } else None
//  }

//  def collectRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Option[RDD[(Long, Iterable[String])]] = {
//    ruleStep match {
//      case step: ConcreteRuleStep if (step.persistType == RecordPersistType) => {
//        val name = step.name
//        try {
//          val pdf = sqlContext.table(s"`${name}`")
//          val cols = pdf.columns
//          val rdd = pdf.flatMap { row =>
//            val values = cols.flatMap { col =>
//              Some((col, row.getAs[Any](col)))
//            }.toMap
//            values.get(GroupByColumn.tmst) match {
//              case Some(t: Long) if (timeGroups.exists(_ == t)) => Some((t, JsonUtil.toJson(values)))
//              case _ => None
//            }
//          }.groupByKey()
//          Some(rdd)
//        } catch {
//          case e: Throwable => {
//            error(s"collect records ${name} error: ${e.getMessage}")
//            None
//          }
//        }
//      }
//      case _ => None
//    }
//  }
//
//  def collectUpdateCacheDatas(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Option[RDD[(Long, Iterable[String])]] = {
//    ruleStep match {
//      case step: ConcreteRuleStep if (step.updateDataSource.nonEmpty) => {
//        val name = step.name
//        try {
//          val pdf = sqlContext.table(s"`${name}`")
//          val cols = pdf.columns
//          val rdd = pdf.flatMap { row =>
//            val values = cols.flatMap { col =>
//              Some((col, row.getAs[Any](col)))
//            }.toMap
//            values.get(GroupByColumn.tmst) match {
//              case Some(t: Long) if (timeGroups.exists(_ == t)) => Some((t, JsonUtil.toJson(values)))
//              case _ => None
//            }
//          }.groupByKey()
//          Some(rdd)
//        } catch {
//          case e: Throwable => {
//            error(s"collect update cache datas ${name} error: ${e.getMessage}")
//            None
//          }
//        }
//      }
//      case _ => None
//    }
//  }

}
