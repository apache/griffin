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

import org.apache.griffin.measure.config.params.user.DataSourceParam
import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.data.source._
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.rules.dsl._
import org.apache.griffin.measure.rules.step._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext

case class SparkSqlEngine(sqlContext: SQLContext, @transient ssc: StreamingContext
                         ) extends DqEngine {

//  def genDataSource(dataSourceParam: DataSourceParam): Option[DirectDataSource] = {
//    DataSourceFactory.genDataSource(sqlContext, ssc, dataSourceParam)
//  }

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    ruleStep match {
      case SparkSqlStep(name, rule, _, _) => {
        try {
          val rdf = sqlContext.sql(rule)
          rdf.registerTempTable(name)
          true
        } catch {
          case e: Throwable => {
            error(s"run spark sql [ ${rule} ] error: ${e.getMessage}")
            false
          }
        }
      }
      case _ => false
    }
  }

  def persistRecords(ruleStep: ConcreteRuleStep, persistFactory: PersistFactory): Boolean = {
//    val curTime = new Date().getTime
//    ruleStep match {
//      case SparkSqlStep(name, _, _, RecordPersistType) => {
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
    true
  }

  def collectMetrics(ruleStep: ConcreteRuleStep): Map[Long, Map[String, Any]] = {
    val emptyMap = Map[String, Any]()
    ruleStep match {
      case SparkSqlStep(name, _, _, MetricPersistType) => {
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
            error(s"persist result ${name} error: ${e.getMessage}")
//            emptyMap
            Map[Long, Map[String, Any]]()
          }
        }
      }
//      case _ => emptyMap
      case _ => Map[Long, Map[String, Any]]()
    }
  }

//  def persistResults(ruleSteps: Seq[ConcreteRuleStep], persist: Persist, persistType: PersistType): Boolean = {
//    val curTime = new Date().getTime
//    persistType match {
//      case RecordPersistType => {
//        ;
//      }
//    }
//
//
//    ruleStep match {
//      case SparkSqlStep(name, _, persistType) => {
//        try {
//          persistType match {
//            case RecordPersistType => {
//              val pdf = sqlContext.table(s"`${name}`")
//              val records = pdf.toJSON
//
//              persist.persistRecords(records, name)
//
//              val recordLog = s"[ ${name} ] persist records"
//              persist.log(curTime, recordLog)
//            }
//            case MetricPersistType => {
//              val pdf = sqlContext.table(s"`${name}`")
//              val recordRdd = pdf.toJSON
//
//              val metrics = recordRdd.collect
//              persist.persistMetrics(metrics, name)
//
//              val metricLog = s"[ ${name} ] persist metric \n${metrics.mkString("\n")}"
//              persist.log(curTime, metricLog)
//            }
//            case _ => {
//              val nonLog = s"[ ${name} ] not persisted"
//              persist.log(curTime, nonLog)
//            }
//          }
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
//  }

}




