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
//package org.apache.griffin.measure.cache.metric
//
//import org.apache.griffin.measure.log.Loggable
//import org.apache.griffin.measure.result._
//
//import scala.collection.mutable.{Map => MutableMap}
//
//case class CacheMetricProcesser() extends Loggable {
//
//  val cacheGroup: MutableMap[Long, CacheMetric] = MutableMap()
//
//  def genUpdateCacheMetric(timeGroup: Long, updateTime: Long, metric: Map[String, Any]): Option[CacheMetric] = {
//    cacheGroup.get(timeGroup) match {
//      case Some(cr) => {
//        if (cr.olderThan(updateTime)) {
//          val existMetric = cr.metric
//          val newMetric = existMetric
//          val newMetric = existMetric.update(result.asInstanceOf[existResult.T])
//          if (existResult.differsFrom(newResult)) {
//            Some(CacheMetric(timeGroup, updateTime, newResult))
//          } else None
//        } else None
//      }
//      case _ => {
//        Some(CacheMetric(timeGroup, updateTime, result))
//      }
//    }
//  }
//
//  def update(cr: CacheMetric): Unit = {
//    val t = cr.timeGroup
//    cacheGroup.get(t) match {
//      case Some(c) => {
//        if (c.olderThan(cr.updateTime)) cacheGroup += (t -> cr)
//      }
//      case _ => cacheGroup += (t -> cr)
//    }
//  }
//
//  def getCacheResult(timeGroup: Long): Option[CacheMetric] = {
//    cacheGroup.get(timeGroup)
//  }
//
//  def refresh(overtime: Long): Unit = {
//    val curCacheGroup = cacheGroup.toMap
//    val deadCache = curCacheGroup.filter { pr =>
//      val (_, cr) = pr
//      cr.timeGroup < overtime || cr.result.eventual()
//    }
//    info(s"=== dead cache group count: ${deadCache.size} ===")
//    deadCache.keySet.foreach(cacheGroup -= _)
//  }
//
//}
