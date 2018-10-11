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
package org.apache.griffin.measure.datasource

import org.apache.spark.sql._

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.context.{DQContext, TimeRange}
import org.apache.griffin.measure.datasource.cache.StreamingCacheClient
import org.apache.griffin.measure.datasource.connector.DataConnector
import org.apache.griffin.measure.utils.DataFrameUtil._

/**
  * data source
  * @param name     name of data source
  * @param dsParam  param of this data source
  * @param dataConnectors       list of data connectors
  * @param streamingCacheClientOpt   streaming data cache client option
  */
case class DataSource(name: String,
                      dsParam: DataSourceParam,
                      dataConnectors: Seq[DataConnector],
                      streamingCacheClientOpt: Option[StreamingCacheClient]
                     ) extends Loggable with Serializable {

  val isBaseline: Boolean = dsParam.isBaseline

  def init(): Unit = {
    dataConnectors.foreach(_.init)
  }

  def loadData(context: DQContext): TimeRange = {
    info(s"load data [${name}]")
    val timestamp = context.contextId.timestamp
    val (dfOpt, timeRange) = data(timestamp)
    dfOpt match {
      case Some(df) =>
        context.runTimeTableRegister.registerTable(name, df)
      case None =>
        warn(s"load data source [${name}] fails")
    }
    timeRange
  }

  private def data(timestamp: Long): (Option[DataFrame], TimeRange) = {
    val batches = dataConnectors.flatMap { dc =>
      val (dfOpt, timeRange) = dc.data(timestamp)
      dfOpt match {
        case Some(df) => Some((dfOpt, timeRange))
        case _ => None
      }
    }
    val caches = streamingCacheClientOpt match {
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

  def updateData(df: DataFrame): Unit = {
    streamingCacheClientOpt.foreach(_.updateData(Some(df)))
  }

  def cleanOldData(): Unit = {
    streamingCacheClientOpt.foreach(_.cleanOutTimeData)
  }

  def processFinish(): Unit = {
    streamingCacheClientOpt.foreach(_.processFinish)
  }

}
