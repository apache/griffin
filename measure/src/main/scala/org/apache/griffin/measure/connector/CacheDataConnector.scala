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

import org.apache.griffin.measure.cache.{InfoCacheInstance, ZKInfoCache}
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait CacheDataConnector extends DataConnector {

  def saveData(df: DataFrame, ms: Long): Unit

  def readData(): Try[DataFrame]

  val cacheInfoPath: String
  val readyTimeInterval: Long
  val readyTimeDelay: Long

  def CacheTime = s"${cacheInfoPath}/cache.time"
  def LastProcTime = s"${cacheInfoPath}/last.proc.time"
  def ReadyTime = s"${cacheInfoPath}/ready.time"

  protected def submitCacheTime(ms: Long): Unit = {
    val map = Map[String, String]((CacheTime -> ms.toString))
    InfoCacheInstance.cacheInfo(map)
  }

  protected def submitReadyTime(ms: Long): Unit = {
    if (ms % readyTimeInterval == 0) {
      val map = Map[String, String]((ReadyTime -> ms.toString))
      InfoCacheInstance.cacheInfo(map)
    }
  }

  def submitLastProcTime(ms: Long): Unit = {
    val map = Map[String, String]((LastProcTime -> ms.toString))
    InfoCacheInstance.cacheInfo(map)
  }

  protected def readTimeRange(): (Long, Long) = {
    val map = InfoCacheInstance.readInfo(List(LastProcTime, ReadyTime))
    val lastProcTime = getLong(map, LastProcTime)
    val curReadyTime = getLong(map, ReadyTime)
    (lastProcTime + 1, curReadyTime)
  }

  private def getLong(map: Map[String, String], key: String): Long = {
    try {
      map.get(key) match {
        case Some(v) => v.toLong
        case _ => -1
      }
    } catch {
      case _ => -1
    }
  }

}
