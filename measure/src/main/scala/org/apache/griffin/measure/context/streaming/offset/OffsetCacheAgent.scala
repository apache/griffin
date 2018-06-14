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
package org.apache.griffin.measure.context.streaming.offset

import org.apache.griffin.measure.configuration.params.InfoCacheParam
import org.apache.griffin.measure.context.streaming.lock.{CacheLock, MultiCacheLock}

object OffsetCacheAgent extends OffsetCache {
  var infoCaches: List[OffsetCache] = Nil

  def initAgent(infoCacheParams: Iterable[InfoCacheParam], metricName: String) = {
    val fac = InfoCacheFactory(infoCacheParams, metricName)
    infoCaches = infoCacheParams.flatMap(param => fac.getInfoCache(param)).toList
  }

  def init(): Unit = infoCaches.foreach(_.init)
  def available(): Boolean = infoCaches.foldLeft(false)(_ || _.available)
  def close(): Unit = infoCaches.foreach(_.close)

  def cacheInfo(info: Map[String, String]): Boolean = {
    infoCaches.foldLeft(false) { (res, infoCache) => res || infoCache.cacheInfo(info) }
  }
  def readInfo(keys: Iterable[String]): Map[String, String] = {
    val maps = infoCaches.map(_.readInfo(keys)).reverse
    maps.fold(Map[String, String]())(_ ++ _)
  }
  def deleteInfo(keys: Iterable[String]): Unit = infoCaches.foreach(_.deleteInfo(keys))
  def clearInfo(): Unit = infoCaches.foreach(_.clearInfo)

  def listKeys(path: String): List[String] = {
    infoCaches.foldLeft(Nil: List[String]) { (res, infoCache) =>
      if (res.size > 0) res else infoCache.listKeys(path)
    }
  }

  def genLock(s: String): CacheLock = MultiCacheLock(infoCaches.map(_.genLock(s)))

  object Keys {
    ;
  }
}
