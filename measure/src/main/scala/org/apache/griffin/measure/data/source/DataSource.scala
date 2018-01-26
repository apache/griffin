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
package org.apache.griffin.measure.data.source

import org.apache.griffin.measure.cache.tmst._
import org.apache.griffin.measure.data.connector._
import org.apache.griffin.measure.data.connector.batch._
import org.apache.griffin.measure.data.connector.streaming._
import org.apache.griffin.measure.data.source.cache.OldDataSourceCache
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.temp.{DataFrameCaches, TableRegisters, TimeRange}
import org.apache.griffin.measure.rule.plan.TimeInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class DataSource(sqlContext: SQLContext,
                      name: String,
                      baseline: Boolean,
                      dataConnectors: Seq[DataConnector],
                      dataSourceCacheOpt: Option[OldDataSourceCache]
                     ) extends Loggable with Serializable {

  val batchDataConnectors = DataConnectorFactory.filterBatchDataConnectors(dataConnectors)
  val streamingDataConnectors = DataConnectorFactory.filterStreamingDataConnectors(dataConnectors)
  streamingDataConnectors.foreach(_.dataSourceCacheOpt = dataSourceCacheOpt)

  val tmstCache: TmstCache = TmstCache()

  def init(): Unit = {
    dataSourceCacheOpt.foreach(_.init)
    dataConnectors.foreach(_.init)

    dataSourceCacheOpt.map(_.tmstCache = tmstCache)
    dataConnectors.map(_.tmstCache = tmstCache)
  }

  def loadData(timeInfo: TimeInfo): TimeRange = {
    val calcTime = timeInfo.calcTime
    println(s"load data [${name}]")
    val (dfOpt, tmsts) = data(calcTime)
    dfOpt match {
      case Some(df) => {
//        DataFrameCaches.cacheDataFrame(timeInfo.key, name, df)
        TableRegisters.registerRunTempTable(df, timeInfo.key, name)
      }
      case None => {
        warn(s"load data source [${name}] fails")
      }
    }
    tmsts
  }

  private def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val batches = batchDataConnectors.flatMap { dc =>
      val (dfOpt, timeRange) = dc.data(ms)
      dfOpt match {
        case Some(df) => Some((dfOpt, timeRange))
        case _ => None
      }
    }
    val caches = dataSourceCacheOpt match {
      case Some(dsc) => dsc.readData() :: Nil
      case _ => Nil
    }
    val pairs = batches ++ caches

    if (pairs.size > 0) {
      pairs.reduce { (a, b) =>
        (unionDfOpts(a._1, b._1), a._2.merge(b._2))
      }
    } else {
      (None, TimeRange.emptyTimeRange)
    }
  }

  private def unionDfOpts(dfOpt1: Option[DataFrame], dfOpt2: Option[DataFrame]
                         ): Option[DataFrame] = {
    (dfOpt1, dfOpt2) match {
      case (Some(df1), Some(df2)) => Some(unionDataFrames(df1, df2))
      case (Some(df1), _) => dfOpt1
      case (_, Some(df2)) => dfOpt2
      case _ => None
    }
  }

  private def unionDataFrames(df1: DataFrame, df2: DataFrame): DataFrame = {
    try {
      val cols = df1.columns
      val rdd2 = df2.map{ row =>
        val values = cols.map { col =>
          row.getAs[Any](col)
        }
        Row(values: _*)
      }
      val ndf2 = sqlContext.createDataFrame(rdd2, df1.schema)
      df1 unionAll ndf2
    } catch {
      case e: Throwable => df1
    }
  }

  def updateData(df: DataFrame, ms: Long): Unit = {
    dataSourceCacheOpt.foreach(_.updateData(df, ms))
  }

  def updateDataMap(dfMap: Map[Long, DataFrame]): Unit = {
    dataSourceCacheOpt.foreach(_.updateDataMap(dfMap))
  }

  def cleanOldData(): Unit = {
    dataSourceCacheOpt.foreach(_.cleanOldData)
  }

}
