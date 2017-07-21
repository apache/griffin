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

import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.cache.lock.CacheLock
import org.apache.griffin.measure.config.params.user.DataCacheParam
import org.apache.griffin.measure.result.TimeStampInfo
import org.apache.griffin.measure.rule.DataTypeCalculationUtil
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.util.{Success, Try}

case class DfCacheDataConnector(sqlContext: SQLContext, dataCacheParam: DataCacheParam
                               ) extends CacheDataConnector {

  val config = dataCacheParam.config
  val InfoPath = "info.path"
  val cacheInfoPath: String = config.getOrElse(InfoPath, defCacheInfoPath).toString

  val newCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.new")
  val oldCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.old")

  val timeRangeParam: List[String] = if (dataCacheParam.timeRange != null) dataCacheParam.timeRange else Nil
  val deltaTimeRange: (Long, Long) = (timeRangeParam ::: List("0", "0")) match {
    case s :: e :: _ => {
      val ns = TimeUtil.milliseconds(s) match {
        case Some(n) if (n < 0) => n
        case _ => 0
      }
      val ne = TimeUtil.milliseconds(e) match {
        case Some(n) if (n < 0) => n
        case _ => 0
      }
      (ns, ne)
    }
    case _ => (0, 0)
  }

  val CacheLevel = "cache.level"
  val cacheLevel: String = config.getOrElse(CacheLevel, "MEMORY_AND_DISK").toString

  val timeStampColumn = TimeStampInfo.key

  var newDataFrame: DataFrame = null
  var oldDataFrame: DataFrame = null

  val ReadyTimeInterval = "ready.time.interval"
  val ReadyTimeDelay = "ready.time.delay"
  val readyTimeInterval: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeInterval, "1m").toString).getOrElse(60000L)
  val readyTimeDelay: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeDelay, "1m").toString).getOrElse(60000L)

  def available(): Boolean = {
    true
  }

  def saveData(rdd: RDD[Map[String, Any]], ms: Long): Unit = {
    val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
    if (newCacheLocked) {
      try {
        if (newDataFrame == null) {
          if (!rdd.isEmpty) {
            newDataFrame = genDataFrame(rdd)
            newDataFrame.persist(StorageLevel.fromString(cacheLevel))
          }
        } else {
          if (!rdd.isEmpty) {
            newDataFrame.unpersist()
            newDataFrame = newDataFrame.unionAll(genDataFrame(rdd))
            newDataFrame.persist(StorageLevel.fromString(cacheLevel))
          }
        }
        // submit ms
        submitCacheTime(ms)
        submitReadyTime(ms)
      } catch {
        case e: Throwable => error(s"save data error: ${e.getMessage}")
      } finally {
        newCacheLock.unlock()
      }
    }
  }

  def readData(): Try[RDD[Map[String, Any]]] = Try {
    val timeRange = TimeInfoCache.getTimeRange
    println(s"timeRange: ${timeRange}")
    submitLastProcTime(timeRange._2)
    val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
    println(s"reviseTimeRange: ${reviseTimeRange}")

    // move new data frame to temp data frame
    val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
    val newTempDataFrame = if (newCacheLocked) {
      try {
        val tmp = newDataFrame.filter(s"${timeStampColumn} BETWEEN ${reviseTimeRange._1} AND ${reviseTimeRange._2}")
        newDataFrame.unpersist()
        newDataFrame = newDataFrame.filter(s"${timeStampColumn} > ${reviseTimeRange._2}")
        tmp
      } catch {
        case _ => null
      } finally {
        newCacheLock.unlock()
      }
    } else null

    // add temp data frame to old data frame
    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
    val oldTempDataFrame = if (oldCacheLocked) {
      try {
        if (oldDataFrame != null) {
          oldDataFrame.filter(s"${timeStampColumn} BETWEEN ${reviseTimeRange._1} AND ${reviseTimeRange._2}")
        } else null
      } catch {
        case _ => null
      } finally {
        oldCacheLock.unlock()
      }
    } else null

    val resultDataFrame = if (oldTempDataFrame == null && newTempDataFrame == null) {
      throw new Exception("data not cached")
    } else {
      val finalDataFrame = if (newTempDataFrame == null) {
        oldTempDataFrame
      } else if (oldTempDataFrame == null) {
        newTempDataFrame
      } else {
        oldTempDataFrame.unionAll(newTempDataFrame)
      }
      finalDataFrame
    }

    // data frame -> rdd
    resultDataFrame.map { row =>
      SparkRowFormatter.formatRow(row)
    }
  }

  override def cleanOldData(): Unit = {
    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
    if (oldCacheLocked) {
      try {
        val timeRange = TimeInfoCache.getTimeRange
        val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
        println(s"clean reviseTimeRange: ${reviseTimeRange}")

        oldDataFrame.unpersist()
        oldDataFrame = oldDataFrame.filter(s"${timeStampColumn} >= ${reviseTimeRange._1}")
        oldDataFrame.persist(StorageLevel.fromString(cacheLevel))
      } catch {
        case e: Throwable => error(s"clean old data error: ${e.getMessage}")
      } finally {
        oldCacheLock.unlock()
      }
    }

//    if (initialed) {
//      val timeRange = TimeInfoCache.getTimeRange
//      val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
//      println(s"clean reviseTimeRange: ${reviseTimeRange}")
//
//      dataFrame.show(10)
//
//      dataFrame.unpersist()
//      dataFrame = dataFrame.filter(s"${timeStampColumn} >= ${reviseTimeRange._1}")
//      dataFrame.persist(StorageLevel.fromString(cacheLevel))
//
//      dataFrame.show(10)
//    }
  }

  override def updateAllOldData(oldRdd: RDD[Map[String, Any]]): Unit = {
    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
    if (oldCacheLocked) {
      try {
        if (oldDataFrame == null) {
          if (!oldRdd.isEmpty) {
            oldDataFrame = genDataFrame(oldRdd)
            oldDataFrame.persist(StorageLevel.fromString(cacheLevel))
          }
        } else {
          if (!oldRdd.isEmpty) {
            oldDataFrame.unpersist()
            oldDataFrame = genDataFrame(oldRdd)
            oldDataFrame.persist(StorageLevel.fromString(cacheLevel))
          } else {
            oldDataFrame.unpersist()
            oldDataFrame = null
          }
        }
      } catch {
        case e: Throwable => error(s"update all old data error: ${e.getMessage}")
      } finally {
        oldCacheLock.unlock()
      }
    }
  }

  // generate DataFrame
  // maybe we can directly use def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame
  // to avoid generate data type by myself, just translate each value into Product
  private def genDataFrame(rdd: RDD[Map[String, Any]]): DataFrame = {
    val fields = rdd.aggregate(Map[String, DataType]())(
      DataTypeCalculationUtil.sequenceDataTypeMap, DataTypeCalculationUtil.combineDataTypeMap
    ).toList.map(f => StructField(f._1, f._2))
    val schema = StructType(fields)
    val datas: RDD[Row] = rdd.map { d =>
      val values = fields.map { field =>
        val StructField(k, dt, _, _) = field
        d.get(k) match {
          case Some(v) => v
          case _ => null
        }
      }
      Row(values: _*)
    }
    val df = sqlContext.createDataFrame(datas, schema)
    df
  }

}

import scala.collection.mutable.{ArrayBuffer}

object SparkRowFormatter {

  def formatRow(row: Row): Map[String, Any] = {
    formatRowWithSchema(row, row.schema)
  }

  private def formatRowWithSchema(row: Row, schema: StructType): Map[String, Any] = {
    formatStruct(schema.fields, row)
  }

  private def formatStruct(schema: Seq[StructField], r: Row) = {
    val paired = schema.zip(r.toSeq)
    paired.foldLeft(Map[String, Any]())((s, p) => s ++ formatItem(p))
  }

  private def formatItem(p: Pair[StructField, Any]): Map[String, Any] = {
    p match {
      case (sf, a) =>
        sf.dataType match {
          case ArrayType(et, _) =>
            Map(sf.name -> (if (a == null) a else formatArray(et, a.asInstanceOf[ArrayBuffer[Any]])))
          case StructType(s) =>
            Map(sf.name -> (if (a == null) a else formatStruct(s, a.asInstanceOf[Row])))
          case _ => Map(sf.name -> a)
        }
    }
  }

  private def formatArray(et: DataType, arr: ArrayBuffer[Any]): Seq[Any] = {
    et match {
      case StructType(s) => arr.map(e => formatStruct(s, e.asInstanceOf[Row]))
      case ArrayType(t, _) =>
        arr.map(e => formatArray(t, e.asInstanceOf[ArrayBuffer[Any]]))
      case _ => arr
    }
  }
}