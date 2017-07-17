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
package org.apache.griffin.measure.connector

import org.apache.griffin.measure.config.params.user.DataCacheParam
import org.apache.griffin.measure.result.TimeStampInfo
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.util.{Success, Try}

case class DfCacheDataConnector(sqlContext: SQLContext, dataCacheParam: DataCacheParam
                               ) extends CacheDataConnector {

  val config = dataCacheParam.config

  val CacheLevel = "cache.level"
  val cacheLevel: String = config.getOrElse(CacheLevel, "MEMORY_ONLY").toString

  val timeStampColumn = TimeStampInfo.key

  var initialed: Boolean = false
  var dataFrame: DataFrame = _

  val InfoPath = "info.path"
  val cacheInfoPath: String = config.getOrElse(InfoPath, "path").toString

  val ReadyTimeInterval = "ready.time.interval"
  val ReadyTimeDelay = "ready.time.delay"
  val readyTimeInterval: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeInterval, "1m").toString).getOrElse(60000L)
  val readyTimeDelay: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeDelay, "1m").toString).getOrElse(60000L)

  def available(): Boolean = {
    true
  }

  def saveData(df: DataFrame, ms: Long): Unit = {
    if (!initialed) {
      dataFrame = df
      dataFrame.persist(StorageLevel.fromString(cacheLevel))
      initialed = true
    } else {
      if (!df.rdd.isEmpty) {
        dataFrame.unpersist()
        dataFrame = dataFrame.unionAll(df)
        dataFrame.persist(StorageLevel.fromString(cacheLevel))
      }
    }

    // submit ms
    submitCacheTime(ms)
    submitReadyTime(ms)
  }

  def readData(): Try[DataFrame] = Try {
    if (initialed) {
      val timeRange = readTimeRange
      submitLastProcTime(timeRange._2)
      dataFrame.filter(s"${timeStampColumn} BETWEEN ${timeRange._1} AND ${timeRange._2}")
    } else {
      throw new Exception("data not cached")
    }
  }

}
