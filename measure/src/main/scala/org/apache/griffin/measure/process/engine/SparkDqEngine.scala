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

import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.rule.dsl.{MetricPersistType, RecordPersistType}
import org.apache.griffin.measure.rule.step._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.sql.{DataFrame, SQLContext}

trait SparkDqEngine extends DqEngine {

  val sqlContext: SQLContext

  def collectMetrics(ruleStep: ConcreteRuleStep): Map[Long, Map[String, Any]] = {
    val emptyMap = Map[String, Any]()
    ruleStep match {
      case step: ConcreteRuleStep if (step.persistType == MetricPersistType) => {
        val name = step.name
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
        } catch {
          case e: Throwable => {
            error(s"collect metrics ${name} error: ${e.getMessage}")
            Map[Long, Map[String, Any]]()
          }
        }
      }
      case _ => Map[Long, Map[String, Any]]()
    }
  }

  def collectRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Map[Long, DataFrame] = {
    ruleStep match {
      case step: ConcreteRuleStep if (step.persistType == RecordPersistType) => {
        val name = step.name
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
      case step: ConcreteRuleStep if (step.updateDataSource.nonEmpty) => {
        val name = step.name
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

}
