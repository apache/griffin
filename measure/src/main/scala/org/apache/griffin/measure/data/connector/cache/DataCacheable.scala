///*
//Licensed to the Apache Software Foundation (ASF) under one
//or more contributor license agreements.  See the NOTICE file
//distributed with this work for additional information
//regarding copyright ownership.  The ASF licenses this file
//to you under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.
//*/
//package org.apache.griffin.measure.data.connector.cache
//
//import java.util.concurrent.atomic.AtomicLong
//
//import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
//
//trait DataCacheable {
//
//  protected val defCacheInfoPath = PathCounter.genPath
//
//  val cacheInfoPath: String
//  val readyTimeInterval: Long
//  val readyTimeDelay: Long
//
//  def selfCacheInfoPath = s"${TimeInfoCache.infoPath}/${cacheInfoPath}"
//
//  def selfCacheTime = TimeInfoCache.cacheTime(selfCacheInfoPath)
//  def selfLastProcTime = TimeInfoCache.lastProcTime(selfCacheInfoPath)
//  def selfReadyTime = TimeInfoCache.readyTime(selfCacheInfoPath)
//  def selfCleanTime = TimeInfoCache.cleanTime(selfCacheInfoPath)
//
//  protected def submitCacheTime(ms: Long): Unit = {
//    val map = Map[String, String]((selfCacheTime -> ms.toString))
//    InfoCacheInstance.cacheInfo(map)
//  }
//
//  protected def submitReadyTime(ms: Long): Unit = {
//    val curReadyTime = ms - readyTimeDelay
//    if (curReadyTime % readyTimeInterval == 0) {
//      val map = Map[String, String]((selfReadyTime -> curReadyTime.toString))
//      InfoCacheInstance.cacheInfo(map)
//    }
//  }
//
//  protected def submitLastProcTime(ms: Long): Unit = {
//    val map = Map[String, String]((selfLastProcTime -> ms.toString))
//    InfoCacheInstance.cacheInfo(map)
//  }
//
//  protected def submitCleanTime(ms: Long): Unit = {
//    val cleanTime = genCleanTime(ms)
//    val map = Map[String, String]((selfCleanTime -> cleanTime.toString))
//    InfoCacheInstance.cacheInfo(map)
//  }
//
//  protected def genCleanTime(ms: Long): Long = ms
//
//  protected def readCleanTime(): Option[Long] = {
//    val key = selfCleanTime
//    val keys = key :: Nil
//    InfoCacheInstance.readInfo(keys).get(key).flatMap { v =>
//      try {
//        Some(v.toLong)
//      } catch {
//        case _ => None
//      }
//    }
//  }
//
//}
//
//object PathCounter {
//  private val counter: AtomicLong = new AtomicLong(0L)
//  def genPath(): String = s"path_${increment}"
//  private def increment(): Long = {
//    counter.incrementAndGet()
//  }
//}