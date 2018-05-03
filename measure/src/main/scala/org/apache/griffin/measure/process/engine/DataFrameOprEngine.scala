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
import org.apache.griffin.measure.data.source.{DataSource, DataSourceFactory}
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.temp.{DataFrameCaches, TableRegisters}
import org.apache.griffin.measure.result.AccuracyResult
import org.apache.griffin.measure.rule.adaptor.InternalColumns
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.griffin.measure.utils.ParamUtil._

import scala.util.Try

import org.apache.spark.sql._

case class DataFrameOprEngine(sqlContext: SQLContext) extends SparkDqEngine {

  def runRuleStep(timeInfo: TimeInfo, ruleStep: RuleStep): Boolean = {
    ruleStep match {
      case rs @ DfOprStep(name, rule, details, _, _) => {
        try {
          val df = rule match {
            case DataFrameOprs._fromJson => DataFrameOprs.fromJson(sqlContext, details)
            case DataFrameOprs._accuracy => DataFrameOprs.accuracy(sqlContext, timeInfo, details)
            case DataFrameOprs._clear => DataFrameOprs.clear(sqlContext, details)
            case _ => throw new Exception(s"df opr [ ${rule} ] not supported")
          }
          if (rs.needCache) DataFrameCaches.cacheDataFrame(timeInfo.key, name, df)
          TableRegisters.registerRunTempTable(df, timeInfo.key, name)
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

}

object DataFrameOprs {

  final val _fromJson = "from_json"
  final val _accuracy = "accuracy"
  final val _clear = "clear"

  object AccuracyOprKeys {
    val _dfName = "df.name"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
  }

  def fromJson(sqlContext: SQLContext, details: Map[String, Any]): DataFrame = {
    val _dfName = "df.name"
    val _colName = "col.name"
    val dfName = details.getOrElse(_dfName, "").toString
    val colNameOpt = details.get(_colName).map(_.toString)

    implicit val encoder = Encoders.STRING

    val df: DataFrame = sqlContext.table(s"`${dfName}`")
    val rdd = colNameOpt match {
      case Some(colName: String) => df.map(r => r.getAs[String](colName))
      case _ => df.map(_.getAs[String](0))
    }
    sqlContext.read.json(rdd) // slow process
  }

  def accuracy(sqlContext: SQLContext, timeInfo: TimeInfo, details: Map[String, Any]): DataFrame = {
    import AccuracyOprKeys._

    val dfName = details.getStringOrKey(_dfName)
    val miss = details.getStringOrKey(_miss)
    val total = details.getStringOrKey(_total)
    val matched = details.getStringOrKey(_matched)

//    val _enableIgnoreCache = "enable.ignore.cache"
//    val enableIgnoreCache = details.getBoolean(_enableIgnoreCache, false)

//    val tmst = InternalColumns.tmst

    val updateTime = new Date().getTime

    def getLong(r: Row, k: String): Option[Long] = {
      try {
        Some(r.getAs[Long](k))
      } catch {
        case e: Throwable => None
      }
    }

    val df = sqlContext.table(s"`${dfName}`")

    val results = df.rdd.flatMap { row =>
      try {
        val tmst = getLong(row, InternalColumns.tmst).getOrElse(timeInfo.calcTime)
        val missCount = getLong(row, miss).getOrElse(0L)
        val totalCount = getLong(row, total).getOrElse(0L)
        val ar = AccuracyResult(missCount, totalCount)
        if (ar.isLegal) Some((tmst, ar)) else None
      } catch {
        case e: Throwable => None
      }
    }.collect

    val updateResults = results.flatMap { pair =>
      val (t, result) = pair
      val updatedCacheResultOpt = CacheResultProcesser.genUpdateCacheResult(t, updateTime, result)
      updatedCacheResultOpt
    }

    // update results
    updateResults.foreach { r =>
      CacheResultProcesser.update(r)
    }

    // generate metrics
    val schema = StructType(Array(
      StructField(InternalColumns.tmst, LongType),
      StructField(miss, LongType),
      StructField(total, LongType),
      StructField(matched, LongType),
      StructField(InternalColumns.record, BooleanType),
      StructField(InternalColumns.empty, BooleanType)
    ))
    val rows = updateResults.map { r =>
      val ar = r.result.asInstanceOf[AccuracyResult]
      Row(r.timeGroup, ar.miss, ar.total, ar.getMatch, !ar.initial, ar.eventual)
    }
    val rowRdd = sqlContext.sparkContext.parallelize(rows)
    val retDf = sqlContext.createDataFrame(rowRdd, schema)

    retDf
  }

  def clear(sqlContext: SQLContext, details: Map[String, Any]): DataFrame = {
    val _dfName = "df.name"
    val dfName = details.getOrElse(_dfName, "").toString

    val df = sqlContext.table(s"`${dfName}`")
    val emptyRdd = sqlContext.sparkContext.emptyRDD[Row]
    sqlContext.createDataFrame(emptyRdd, df.schema)
  }

}



