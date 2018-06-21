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
package org.apache.griffin.measure.datasource.cache

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.context.streaming.offset.OffsetCacheClient

/**
  * timestamp offset of streaming data source cache
  */
trait StreamingOffsetCacheable extends Loggable with Serializable {

  val cacheInfoPath: String
  val readyTimeInterval: Long
  val readyTimeDelay: Long

  def selfCacheInfoPath = s"${OffsetCacheClient.infoPath}/${cacheInfoPath}"

  def selfCacheTime = OffsetCacheClient.cacheTime(selfCacheInfoPath)
  def selfLastProcTime = OffsetCacheClient.lastProcTime(selfCacheInfoPath)
  def selfReadyTime = OffsetCacheClient.readyTime(selfCacheInfoPath)
  def selfCleanTime = OffsetCacheClient.cleanTime(selfCacheInfoPath)
  def selfOldCacheIndex = OffsetCacheClient.oldCacheIndex(selfCacheInfoPath)

  protected def submitCacheTime(ms: Long): Unit = {
    val map = Map[String, String]((selfCacheTime -> ms.toString))
    OffsetCacheClient.cache(map)
  }

  protected def submitReadyTime(ms: Long): Unit = {
    val curReadyTime = ms - readyTimeDelay
    if (curReadyTime % readyTimeInterval == 0) {
      val map = Map[String, String]((selfReadyTime -> curReadyTime.toString))
      OffsetCacheClient.cache(map)
    }
  }

  protected def submitLastProcTime(ms: Long): Unit = {
    val map = Map[String, String]((selfLastProcTime -> ms.toString))
    OffsetCacheClient.cache(map)
  }

  protected def readLastProcTime(): Option[Long] = readSelfInfo(selfLastProcTime)

  protected def submitCleanTime(ms: Long): Unit = {
    val cleanTime = genCleanTime(ms)
    val map = Map[String, String]((selfCleanTime -> cleanTime.toString))
    OffsetCacheClient.cache(map)
  }

  protected def genCleanTime(ms: Long): Long = ms

  protected def readCleanTime(): Option[Long] = readSelfInfo(selfCleanTime)

  protected def submitOldCacheIndex(index: Long): Unit = {
    val map = Map[String, String]((selfOldCacheIndex -> index.toString))
    OffsetCacheClient.cache(map)
  }

  def readOldCacheIndex(): Option[Long] = readSelfInfo(selfOldCacheIndex)

  private def readSelfInfo(key: String): Option[Long] = {
    OffsetCacheClient.read(key :: Nil).get(key).flatMap { v =>
      try {
        Some(v.toLong)
      } catch {
        case _:Throwable => error("try to read not existing value from OffsetCacheClient::readSelfInfo");None
      }
    }
  }

}