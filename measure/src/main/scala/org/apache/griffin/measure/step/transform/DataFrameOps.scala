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
package org.apache.griffin.measure.step.transform

import java.util.Date

import org.apache.griffin.measure.context.ContextId
import org.apache.griffin.measure.context.streaming.metric.CacheResults.CacheResult
import org.apache.griffin.measure.context.streaming.metric._
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SQLContext, _}

/**
  * pre-defined data frame operations
  */
object DataFrameOps {

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

  def accuracy(sqlContext: SQLContext, contextId: ContextId, details: Map[String, Any]): DataFrame = {
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
        val tmst = getLong(row, ConstantColumns.tmst).getOrElse(contextId.timestamp)
        val missCount = getLong(row, miss).getOrElse(0L)
        val totalCount = getLong(row, total).getOrElse(0L)
        val ar = AccuracyMetric(missCount, totalCount)
        if (ar.isLegal) Some((tmst, ar)) else None
      } catch {
        case e: Throwable => None
      }
    }.collect

    // cache and update results
    val updatedResults = CacheResults.update(results.map{ pair =>
      val (t, r) = pair
      CacheResult(t, updateTime, r)
    })

    // generate metrics
    val schema = StructType(Array(
      StructField(ConstantColumns.tmst, LongType),
      StructField(miss, LongType),
      StructField(total, LongType),
      StructField(matched, LongType),
      StructField(ConstantColumns.record, BooleanType),
      StructField(ConstantColumns.empty, BooleanType)
    ))
    val rows = updatedResults.map { r =>
      val ar = r.result.asInstanceOf[AccuracyMetric]
      Row(r.timeStamp, ar.miss, ar.total, ar.getMatch, !ar.initial, ar.eventual)
    }.toArray
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
