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

  def loadData(ms: Long): Set[Long] = {
    val (dfOpt, tmsts) = data(ms)
    dfOpt match {
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
    tmsts
  }

  def dropTable(): Unit = {
    try {
      sqlContext.dropTempTable(name)
    } catch {
      case e: Throwable => warn(s"drop table [${name}] fails")
    }
  }

  private def data(ms: Long): (Option[DataFrame], Set[Long]) = {
//    val batchPairs = batchDataConnectors.map(_.data(ms))
//    println(batchPairs.size)
//    val (batchDataFrameOpt, batchTmsts) = (None, Set.empty[Long])
//    val (batchDataFrameOpt, batchTmsts) = batchDataConnectors.map(_.data(ms)).reduce( (a, b) =>
//      (unionDfOpts(a._1, b._1), a._2 ++ b._2)
//    )
    val batches = batchDataConnectors.flatMap { dc =>
      val (dfOpt, tmsts) = dc.data(ms)
      dfOpt match {
        case Some(df) => Some((dfOpt, tmsts))
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
        (unionDfOpts(a._1, b._1), a._2 ++ b._2)
      }
    } else {
      (None, Set.empty[Long])
    }

//    val (cacheDataFrameOpt, cacheTmsts) = dataSourceCacheOpt match {
//      case Some(dsc) => dsc.readData()
//      case _ => (None, Set.empty[Long])
//    }
//    println("go")

//    (unionDfOpts(batchDataFrameOpt, cacheDataFrameOpt), batchTmsts ++ cacheTmsts)
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
