package org.apache.griffin.measure.connector

import org.apache.griffin.measure.cache.{InfoCacheInstance, ZKInfoCache}
import org.apache.spark.sql.DataFrame

trait CacheDataConnector extends DataConnector {

  def saveData(df: DataFrame, ms: Long): Unit

  def readData(): DataFrame

  val cacheTimeKey: String

  val LastProcTime = "last.proc.time"
  val CurReadyTime = "cur.ready.time"

  protected def submitCacheTime(ms: Long): Unit = {
    val map = Map[String, String]() + (cacheTimeKey -> ms.toString)
    InfoCacheInstance.cacheInfo(map)
  }

  protected def readTimeRange(): (Long, Long) = {
    val map = InfoCacheInstance.readInfo(List(LastProcTime, CurReadyTime))
    val lastProcTime = getLong(map, LastProcTime)
    val curReadyTime = getLong(map, CurReadyTime)
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
