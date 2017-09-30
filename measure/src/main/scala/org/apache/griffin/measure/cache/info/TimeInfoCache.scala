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
package org.apache.griffin.measure.cache.info

import org.apache.griffin.measure.log.Loggable

object TimeInfoCache extends Loggable with Serializable {

  private val CacheTime = "cache.time"
  private val LastProcTime = "last.proc.time"
  private val ReadyTime = "ready.time"
  private val CleanTime = "clean.time"

  def cacheTime(path: String): String = s"${path}/${CacheTime}"
  def lastProcTime(path: String): String = s"${path}/${LastProcTime}"
  def readyTime(path: String): String = s"${path}/${ReadyTime}"
  def cleanTime(path: String): String = s"${path}/${CleanTime}"

  val infoPath = "info"

  val finalCacheInfoPath = "info.final"
  val finalReadyTime = s"${finalCacheInfoPath}/${ReadyTime}"
  val finalLastProcTime = s"${finalCacheInfoPath}/${LastProcTime}"
  val finalCleanTime = s"${finalCacheInfoPath}/${CleanTime}"

  def startTimeInfoCache(): Unit = {
    genFinalReadyTime
  }

  def getTimeRange(): (Long, Long) = {
    readTimeRange
  }

  def getCleanTime(): Long = {
    readCleanTime
  }

  def endTimeInfoCache: Unit = {
    genFinalLastProcTime
    genFinalCleanTime
  }

  private def genFinalReadyTime(): Unit = {
    val subPath = InfoCacheInstance.listKeys(infoPath)
    val keys = subPath.map { p => s"${infoPath}/${p}/${ReadyTime}" }
    val result = InfoCacheInstance.readInfo(keys)
    val time = keys.map { k =>
      getLong(result, k)
    }.min
    val map = Map[String, String]((finalReadyTime -> time.toString))
    InfoCacheInstance.cacheInfo(map)
  }

  private def genFinalLastProcTime(): Unit = {
    val subPath = InfoCacheInstance.listKeys(infoPath)
    val keys = subPath.map { p => s"${infoPath}/${p}/${LastProcTime}" }
    val result = InfoCacheInstance.readInfo(keys)
    val time = keys.map { k =>
      getLong(result, k)
    }.min
    val map = Map[String, String]((finalLastProcTime -> time.toString))
    InfoCacheInstance.cacheInfo(map)
  }

  private def genFinalCleanTime(): Unit = {
    val subPath = InfoCacheInstance.listKeys(infoPath)
    val keys = subPath.map { p => s"${infoPath}/${p}/${CleanTime}" }
    val result = InfoCacheInstance.readInfo(keys)
    val time = keys.map { k =>
      getLong(result, k)
    }.min
    val map = Map[String, String]((finalCleanTime -> time.toString))
    InfoCacheInstance.cacheInfo(map)
  }

  private def readTimeRange(): (Long, Long) = {
    val map = InfoCacheInstance.readInfo(List(finalLastProcTime, finalReadyTime))
    val lastProcTime = getLong(map, finalLastProcTime)
    val curReadyTime = getLong(map, finalReadyTime)
    (lastProcTime + 1, curReadyTime)
  }

  private def readCleanTime(): Long = {
    val map = InfoCacheInstance.readInfo(List(finalCleanTime))
    val cleanTime = getLong(map, finalCleanTime)
    cleanTime
  }

  private def getLong(map: Map[String, String], key: String): Long = {
    try {
      map.get(key) match {
        case Some(v) => v.toLong
        case _ => -1
      }
    } catch {
      case e: Throwable => -1
    }
  }

}
