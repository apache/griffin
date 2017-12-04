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
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.step._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.griffin.measure.utils.ParamUtil._

case class DataFrameOprEngine(sqlContext: SQLContext) extends SparkDqEngine {

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    ruleStep match {
      case DfOprStep(ti, ri) => {
        try {
          ri.rule match {
            case DataFrameOprs._fromJson => {
              val df = DataFrameOprs.fromJson(sqlContext, ri)
              df.registerTempTable(ri.name)
            }
            case DataFrameOprs._accuracy => {
              val df = DataFrameOprs.accuracy(sqlContext, ti, ri)
              df.registerTempTable(ri.name)
            }
            case DataFrameOprs._clear => {
              val df = DataFrameOprs.clear(sqlContext, ri)
              df.registerTempTable(ri.name)
            }
            case _ => {
              throw new Exception(s"df opr [ ${ri.rule} ] not supported")
            }
          }
          true
        } catch {
          case e: Throwable => {
            error(s"run df opr [ ${ri.rule} ] error: ${e.getMessage}")
            false
          }
        }
      }
      case _ => false
    }
  }

}

object DataFrameOprs {

  final val _fromJson = "from_json"
  final val _accuracy = "accuracy"
  final val _clear = "clear"

  def fromJson(sqlContext: SQLContext, ruleInfo: RuleInfo): DataFrame = {
    val details = ruleInfo.details

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

  def accuracy(sqlContext: SQLContext, timeInfo: TimeInfo, ruleInfo: RuleInfo): DataFrame = {
    val details = ruleInfo.details

    val _dfName = "df.name"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
//    val _tmst = "tmst"
    val dfName = details.getStringOrKey(_dfName)
    val miss = details.getStringOrKey(_miss)
    val total = details.getStringOrKey(_total)
    val matched = details.getStringOrKey(_matched)
//    val tmst = details.getOrElse(_tmst, _tmst).toString
//    val tmst = GroupByColumn.tmst

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
      try {
        val missCount = getLong(row, miss)
        val totalCount = getLong(row, total)
        val ar = AccuracyResult(missCount, totalCount)
        if (ar.isLegal) Some((timeInfo.tmst, ar)) else None
      } catch {
        case e: Throwable => None
      }
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
      StructField(miss, LongType),
      StructField(total, LongType),
      StructField(matched, LongType)
    ))
    val rows = updateResults.map { r =>
      val ar = r.result.asInstanceOf[AccuracyResult]
      Row(ar.miss, ar.total, ar.getMatch)
    }
    val rowRdd = sqlContext.sparkContext.parallelize(rows)
    sqlContext.createDataFrame(rowRdd, schema)

  }

  def clear(sqlContext: SQLContext, ruleInfo: RuleInfo): DataFrame = {
    val details = ruleInfo.details

    val _dfName = "df.name"
    val dfName = details.getOrElse(_dfName, "").toString

    val df = sqlContext.table(s"`${dfName}`")
    val emptyRdd = sqlContext.sparkContext.emptyRDD[Row]
    sqlContext.createDataFrame(emptyRdd, df.schema)
  }

}



