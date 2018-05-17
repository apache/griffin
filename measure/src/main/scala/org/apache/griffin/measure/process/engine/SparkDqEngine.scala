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

import org.apache.griffin.measure.process._
import org.apache.griffin.measure.rule.adaptor.InternalColumns
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
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

  def collectMetrics(metricExport: MetricExport): Map[Long, Map[String, Any]] = {
    if (collectable) {
      val MetricExport(name, stepName, collectType, defTmst, mode) = metricExport
      try {
        val metricMaps: Seq[Map[String, Any]] = getMetricMaps(stepName)
        mode match {
          case SimpleMode => {
            val metrics: Map[String, Any] = normalizeMetric(metricMaps, name, collectType)
            emptyMetricMap + (defTmst -> metrics)
          }
          case TimestampMode => {
            val tmstMetrics = metricMaps.map { metric =>
              val tmst = metric.getLong(InternalColumns.tmst, defTmst)
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
      } catch {
        case e: Throwable => {
          error(s"collect metrics ${name} error: ${e.getMessage}")
          emptyMetricMap
        }
      }
    } else emptyMetricMap
  }

  private def getTmst(row: Row, defTmst: Long): Long = {
    try {
      row.getAs[Long](InternalColumns.tmst)
    } catch {
      case _: Throwable => defTmst
    }
  }

  private def getRecordDataFrame(recordExport: RecordExport): Option[DataFrame] = {
    if (collectable) {
      val stepDf = sqlContext.table(s"`${recordExport.stepName}`")
      Some(stepDf)
    } else None
  }

  def collectBatchRecords(recordExport: RecordExport): Option[RDD[String]] = {
    getRecordDataFrame(recordExport).map(_.toJSON.rdd)
  }

  def collectStreamingRecords(recordExport: RecordExport): (Option[RDD[(Long, Iterable[String])]], Set[Long]) = {
    implicit val encoder = Encoders.tuple(Encoders.scalaLong, Encoders.STRING)
    val RecordExport(_, _, _, originDFOpt, defTmst, procType) = recordExport
    getRecordDataFrame(recordExport) match {
      case Some(stepDf) => {
        originDFOpt match {
          case Some(originName) => {
            val tmsts = (stepDf.collect.flatMap { row =>
              try {
                val tmst = getTmst(row, defTmst)
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
                val tmst = getTmst(row, defTmst)
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
              (Some(records.rdd.groupByKey), emptyTmsts)
            } else (None, emptyTmsts)
          }
          case _ => {
            val records = stepDf.flatMap { row =>
              val tmst = getTmst(row, defTmst)
              try {
                val map = SparkRowFormatter.formatRow(row)
                val str = JsonUtil.toJson(map)
                Some((tmst, str))
              } catch {
                case e: Throwable => None
              }
            }
            (Some(records.rdd.groupByKey), Set[Long]())
          }
        }
      }
      case _ => (None, Set[Long]())
    }
  }

  def collectUpdateDf(dsUpdate: DsUpdate): Option[DataFrame] = {
    if (collectable) {
      val DsUpdate(_, stepName) = dsUpdate
      val stepDf = sqlContext.table(s"`${stepName}`")
      Some(stepDf)
    } else None
  }

}
