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

import org.apache.griffin.measure.data.connector._
import org.apache.griffin.measure.data.connector.batch._
import org.apache.griffin.measure.data.connector.streaming._
import org.apache.griffin.measure.log.Loggable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class DataSource(sqlContext: SQLContext,
                      name: String,
                      dataConnectors: Seq[DataConnector],
                      dataSourceCacheOpt: Option[DataSourceCache]
                     ) extends Loggable with Serializable {

  val batchDataConnectors = DataConnectorFactory.filterBatchDataConnectors(dataConnectors)
  val streamingDataConnectors = DataConnectorFactory.filterStreamingDataConnectors(dataConnectors)
  streamingDataConnectors.foreach(_.dataSourceCacheOpt = dataSourceCacheOpt)

  def init(): Unit = {
    dataSourceCacheOpt.foreach(_.init)
    dataConnectors.foreach(_.init)
  }

  def loadData(ms: Long): Unit = {
    data(ms) match {
      case Some(df) => {
        df.registerTempTable(name)
      }
      case None => {
//        val df = sqlContext.emptyDataFrame
//        df.registerTempTable(name)
        warn(s"load data source [${name}] fails")
//        throw new Exception(s"load data source [${name}] fails")
      }
    }
  }

  def dropTable(): Unit = {
    try {
      sqlContext.dropTempTable(name)
    } catch {
      case e: Throwable => warn(s"drop table [${name}] fails")
    }
  }

  private def data(ms: Long): Option[DataFrame] = {
    val batchDataFrameOpt = batchDataConnectors.flatMap { dc =>
      dc.data(ms)
    }.reduceOption((a, b) => unionDataFrames(a, b))

    val cacheDataFrameOpt = dataSourceCacheOpt.flatMap(_.readData())

    (batchDataFrameOpt, cacheDataFrameOpt) match {
      case (Some(bdf), Some(cdf)) => Some(unionDataFrames(bdf, cdf))
      case (Some(bdf), _) => Some(bdf)
      case (_, Some(cdf)) => Some(cdf)
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
//      df1 unionAll df2
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
