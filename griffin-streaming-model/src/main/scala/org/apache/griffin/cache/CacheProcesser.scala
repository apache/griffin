package org.apache.griffin.cache

import org.apache.griffin.config.params.RetryParam

import scala.collection.mutable.{Map => MutableMap}

case class CacheProcesser(retryParam: RetryParam) {

  val cacheGroup: MutableMap[Long, CachedData] = MutableMap()

  val needRetry = retryParam.needRetry
  val nextRetryInterval = retryParam.nextRetryInterval * 1000

  def cache(timeStamp: Long, cacheData: CachedData): Unit = {
    if (!cacheData.cacheEnd) {
      cacheGroup.get(timeStamp) match {
        case Some(cd) => {
          println("unnecessary to cache")
        }
        case _ => {
          cacheGroup += (timeStamp -> cacheData)
          println(s"add cache ${timeStamp}: ${cacheData.result}")
        }
      }
    } else {
      println("unnecessary to cache")
    }
  }

//  def process(time: Long): Unit = {
//    val curCacheGroup = cacheGroup.toMap
//    val fireCaches = curCacheGroup.filter { pair => (time >= pair._2.nextFireTime) }
//    println(s"=== retry group count: ${fireCaches.size} ===")
//    fireCaches.foreach { pair =>
//      val (_, cacheData) = pair
//      println(s"=== retry: ${pair._1} ===")
//      cacheData.recalcAndCache(time)
//      cacheData.nextFireTime = time + nextRetryInterval
//    }
//  }

  def refresh(t: Long): Unit = {
    val curCacheGroup = cacheGroup.toMap
    curCacheGroup.foreach(_._2.curTime = t)
    val deadCache = curCacheGroup.filter(_._2.cacheEnd)
    println(s"=== dead cache group count: ${deadCache.size} ===")
    deadCache.keySet.foreach(cacheGroup -= _)
  }

  def getCache(timeStamp: Long): Option[CachedData] = {
    cacheGroup.get(timeStamp)
  }

}
