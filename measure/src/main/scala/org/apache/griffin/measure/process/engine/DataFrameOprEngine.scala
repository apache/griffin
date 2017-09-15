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

import java.util.Date

import org.apache.griffin.measure.cache.result.CacheResultProcesser
import org.apache.griffin.measure.config.params.user.DataSourceParam
import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.data.source.{DataSource, DataSourceFactory}
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.result.AccuracyResult
import org.apache.griffin.measure.rules.dsl._
import org.apache.griffin.measure.rules.step._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext

case class DataFrameOprEngine(sqlContext: SQLContext, @transient ssc: StreamingContext
                             ) extends DqEngine {

//  def genDataSource(dataSourceParam: DataSourceParam): Option[DirectDataSource] = {
//    DataSourceFactory.genDataSource(sqlContext, ssc, dataSourceParam)
//  }

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    ruleStep match {
      case DfOprStep(name, rule, details, _, _) => {
        try {
          rule match {
            case DataFrameOprs._fromJson => {
              val df = DataFrameOprs.fromJson(sqlContext, details)
              df.registerTempTable(name)
            }
            case DataFrameOprs._accuracy => {
              val df = DataFrameOprs.accuracy(sqlContext, details)
              df.registerTempTable(name)
            }
            case DataFrameOprs._clear => {
              val df = DataFrameOprs.clear(sqlContext, details)
              df.registerTempTable(name)
            }
            case _ => {
              throw new Exception(s"df opr [ ${rule} ] not supported")
            }
          }
          true
        } catch {
          case e: Throwable => {
            error(s"run df opr [ ${rule} ] error: ${e.getMessage}")
            false
          }
        }
      }
      case _ => false
    }
  }

//  def persistRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long], persistFactory: PersistFactory): Boolean = {
//    val curTime = new Date().getTime
//    ruleStep match {
//      case DfOprStep(name, _, _, RecordPersistType) => {
//        try {
//          val pdf = sqlContext.table(s"`${name}`")
//          val records = pdf.toJSON
//
//          persist.persistRecords(records, name)
//
//          val recordLog = s"[ ${name} ] persist records"
//          persist.log(curTime, recordLog)
//
//          true
//        } catch {
//          case e: Throwable => {
//            error(s"persist result ${name} error: ${e.getMessage}")
//            false
//          }
//        }
//      }
//      case _ => false
//    }
//    true
//  }

  def collectRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Map[Long, DataFrame] = {
    ruleStep match {
      case DfOprStep(name, _, _, RecordPersistType, _) => {
        try {
          val pdf = sqlContext.table(s"`${name}`")
          timeGroups.flatMap { timeGroup =>
            try {
              val tdf = pdf.filter(s"`${GroupByColumn.tmst}` = ${timeGroup}")
              Some((timeGroup, tdf))
            } catch {
              case e: Throwable => None
            }
          }.toMap
        } catch {
          case e: Throwable => {
            error(s"collect records ${name} error: ${e.getMessage}")
            Map[Long, DataFrame]()
          }
        }
      }
      case _ => Map[Long, DataFrame]()
    }
  }

  def collectUpdateCacheDatas(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Map[Long, DataFrame] = {
    ruleStep match {
      case DfOprStep(name, _, _, _, Some(ds)) => {
        try {
          val pdf = sqlContext.table(s"`${name}`")
          timeGroups.flatMap { timeGroup =>
            try {
              val tdf = pdf.filter(s"`${GroupByColumn.tmst}` = ${timeGroup}")
              Some((timeGroup, tdf))
            } catch {
              case e: Throwable => None
            }
          }.toMap
        } catch {
          case e: Throwable => {
            error(s"collect update cache datas ${name} error: ${e.getMessage}")
            Map[Long, DataFrame]()
          }
        }
      }
      case _ => Map[Long, DataFrame]()
    }
  }

  def collectMetrics(ruleStep: ConcreteRuleStep): Map[Long, Map[String, Any]] = {
    val emptyMap = Map[String, Any]()
    ruleStep match {
      case DfOprStep(name, _, _, MetricPersistType, _) => {
        try {
          val pdf = sqlContext.table(s"`${name}`")
          val records = pdf.toJSON.collect()

          val pairs = records.flatMap { rec =>
            try {
              val value = JsonUtil.toAnyMap(rec)
              value.get(GroupByColumn.tmst) match {
                case Some(t) => {
                  val key = t.toString.toLong
                  Some((key, value))
                }
                case _ => None
              }
            } catch {
              case e: Throwable => None
            }
          }
          val groupedPairs = pairs.foldLeft(Map[Long, Seq[Map[String, Any]]]()) { (ret, pair) =>
            val (k, v) = pair
            ret.get(k) match {
              case Some(seq) => ret + (k -> (seq :+ v))
              case _ => ret + (k -> (v :: Nil))
            }
          }
          groupedPairs.mapValues { vs =>
            if (vs.size > 1) {
              Map[String, Any]((name -> vs))
            } else {
              vs.headOption.getOrElse(emptyMap)
            }
          }

//          if (ruleStep.isGroupMetric) {
//            val arr = records.flatMap { rec =>
//              try {
//                Some(JsonUtil.toAnyMap(rec))
//              } catch {
//                case e: Throwable => None
//              }
//            }
//            Map[String, Any]((name -> arr))
//          } else {
//            records.headOption match {
//              case Some(head) => {
//                try {
//                  JsonUtil.toAnyMap(head)
//                } catch {
//                  case e: Throwable => emptyMap
//                }
//              }
//              case _ => emptyMap
//            }
//          }
        } catch {
          case e: Throwable => {
            error(s"collect metrics ${name} error: ${e.getMessage}")
//            emptyMap
            Map[Long, Map[String, Any]]()
          }
        }
      }
//      case _ => emptyMap
      case _ => Map[Long, Map[String, Any]]()
    }
  }

//  def persistResults(ruleStep: ConcreteRuleStep, persist: Persist): Boolean = {
//    val curTime = new Date().getTime
//    ruleStep match {
//      case DfOprStep(name, _, _) => {
//        try {
//          val nonLog = s"[ ${name} ] not persisted"
//          persist.log(curTime, nonLog)
//
//          true
//        } catch {
//          case e: Throwable => {
//            error(s"persist result ${ruleStep.name} error: ${e.getMessage}")
//            false
//          }
//        }
//      }
//      case _ => false
//    }
//  }

}

object DataFrameOprs {

  final val _fromJson = "from_json"
  final val _accuracy = "accuracy"
  final val _clear = "clear"

  def fromJson(sqlContext: SQLContext, details: Map[String, Any]): DataFrame = {
    val _dfName = "df.name"
    val _colName = "col.name"
    val dfName = details.getOrElse(_dfName, "").toString
    val colNameOpt = details.get(_colName).map(_.toString)

    val df = sqlContext.table(s"`${dfName}`")
    val rdd = colNameOpt match {
      case Some(colName: String) => df.map(_.getAs[String](colName))
      case _ => df.map(_.getAs[String](0))
    }
    sqlContext.read.json(rdd)
  }

  def accuracy(sqlContext: SQLContext, details: Map[String, Any]): DataFrame = {
    val _dfName = "df.name"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
    val _tmst = "tmst"
    val dfName = details.getOrElse(_dfName, _dfName).toString
    val miss = details.getOrElse(_miss, _miss).toString
    val total = details.getOrElse(_total, _total).toString
    val matched = details.getOrElse(_matched, _matched).toString
    val tmst = details.getOrElse(_tmst, _tmst).toString

    val updateTime = new Date().getTime

    def getLong(r: Row, k: String): Long = {
      try {
        r.getAs[Long](k)
      } catch {
        case e: Throwable => 0L
      }
    }

    val df = sqlContext.table(s"`${dfName}`")
    val results = df.flatMap { row =>
      val t = getLong(row, tmst)
      if (t > 0) {
        val missCount = getLong(row, miss)
        val totalCount = getLong(row, total)
        val ar = AccuracyResult(missCount, totalCount)
        Some((t, ar))
      } else None
    }.collect

    val updateResults = results.flatMap { pair =>
      val (t, result) = pair
      val updatedCacheResultOpt = CacheResultProcesser.genUpdateCacheResult(t, updateTime, result)
      updatedCacheResultOpt
    }

    // update
    updateResults.foreach { r =>
      CacheResultProcesser.update(r)
    }

    val schema = StructType(Array(
      StructField(tmst, LongType),
      StructField(miss, LongType),
      StructField(total, LongType),
      StructField(matched, LongType)
    ))
    val rows = updateResults.map { r =>
      val ar = r.result.asInstanceOf[AccuracyResult]
      Row(r.timeGroup, ar.miss, ar.total, ar.getMatch)
    }
    val rowRdd = sqlContext.sparkContext.parallelize(rows)
    sqlContext.createDataFrame(rowRdd, schema)

  }

  def clear(sqlContext: SQLContext, details: Map[String, Any]): DataFrame = {
    val _dfName = "df.name"
    val dfName = details.getOrElse(_dfName, "").toString

    val df = sqlContext.table(s"`${dfName}`")
    val emptyRdd = sqlContext.sparkContext.emptyRDD[Row]
    sqlContext.createDataFrame(emptyRdd, df.schema)
  }

}



