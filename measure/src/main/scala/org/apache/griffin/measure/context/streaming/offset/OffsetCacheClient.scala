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

import org.apache.griffin.measure.configuration.dqdefinition.OffsetCacheParam
import org.apache.griffin.measure.context.streaming.lock.{CacheLock, CacheLockSeq}

object OffsetCacheClient extends OffsetCache with OffsetOps {
  var offsetCaches: Seq[OffsetCache] = Nil

  def initClient(offsetCacheParams: Iterable[OffsetCacheParam], metricName: String) = {
    val fac = OffsetCacheFactory(offsetCacheParams, metricName)
    offsetCaches = offsetCacheParams.flatMap(param => fac.getOffsetCache(param)).toList
  }

  def init(): Unit = offsetCaches.foreach(_.init)
  def available(): Boolean = offsetCaches.foldLeft(false)(_ || _.available)
  def close(): Unit = offsetCaches.foreach(_.close)

  def cache(kvs: Map[String, String]): Unit = {
    offsetCaches.foreach(_.cache(kvs))
  }
  def read(keys: Iterable[String]): Map[String, String] = {
    val maps = offsetCaches.map(_.read(keys)).reverse
    maps.fold(Map[String, String]())(_ ++ _)
  }
  def delete(keys: Iterable[String]): Unit = offsetCaches.foreach(_.delete(keys))
  def clear(): Unit = offsetCaches.foreach(_.clear)

  def listKeys(path: String): List[String] = {
    offsetCaches.foldLeft(Nil: List[String]) { (res, offsetCache) =>
      if (res.size > 0) res else offsetCache.listKeys(path)
    }
  }

  def genLock(s: String): CacheLock = CacheLockSeq(offsetCaches.map(_.genLock(s)))

}
