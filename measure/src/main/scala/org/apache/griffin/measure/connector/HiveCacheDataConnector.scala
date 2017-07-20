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
import org.apache.griffin.measure.config.params.user.DataCacheParam
import org.apache.griffin.measure.result.TimeStampInfo
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.util.Try

case class HiveCacheDataConnector(sqlContext: SQLContext, dataCacheParam: DataCacheParam
                                 ) extends CacheDataConnector {

  if (!sqlContext.isInstanceOf[HiveContext]) {
    throw new Exception("hive context not prepared!")
  }

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

  val TableName = "table.name"
  val tableName: String = config.get(TableName) match {
    case Some(s: String) => s
    case _ => throw new Exception("invalid table.name!")
  }
  val ParentPath = "parent.path"
  val parentPath: String = config.get(ParentPath) match {
    case Some(s: String) => s
    case _ => throw new Exception("invalid parent.path!")
  }
  val tablePath = getFilePath(parentPath, tableName)

  val timeStampColumn = TimeStampInfo.key

  val ReadyTimeInterval = "ready.time.interval"
  val ReadyTimeDelay = "ready.time.delay"
  val readyTimeInterval: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeInterval, "1m").toString).getOrElse(60000L)
  val readyTimeDelay: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeDelay, "1m").toString).getOrElse(60000L)

  type Schema = (Long, String)
  val schema: List[(String, String)] = List(
    ("tmst", "bigint"),
    ("payload", "string")
  )
  type Partition = (Long, Long)
  val partition: List[(String, String, String)] = List(
    ("hr", "bigint", "hour"),
    ("min", "bigint", "min")
  )

  private val fieldSep = ""","""
  private val rowSep = """\n"""

  protected def getFilePath(parentPath: String, fileName: String): String = {
    if (parentPath.endsWith("/")) parentPath + fileName else parentPath + "/" + fileName
  }

  override def init(): Unit = {
    val colsSql = schema.map { field =>
      s"`${field._1}` ${field._2}"
    }.mkString(", ")
    val partitionsSql = partition.map { partition =>
      s"`${partition._1}` ${partition._2}"
    }
    val sql = s"""CREATE EXTERNAL TABLE IF NOT EXISTS `${tableName}`
                  |(${colsSql}) PARTITIONED BY (${partitionsSql})
                  |ROW FORMAT DELIMITED
                  |FIELDS TERMINATED BY '${fieldSep}'
                  |LINES TERMINATED BY '${rowSep}'
                  |STORED AS TEXTFILE
                  |LOCATION '${tablePath}'""".stripMargin
    sqlContext.sql(sql)
  }

  def available(): Boolean = {
    true
  }

  private def encode(data: Map[String, Any]): Schema = {
    ;
  }

  private def decode(data: Schema): Map[String, Any] = {
    ;
  }

  private def getPartition(ms: Long): List[(String, Any)] = {
    partition.map { p =>
      val (name, _, unit) = p
      val t = TimeUtil.timeToUnit(ms, unit)
      (name, t)
    }
  }
  private def genPartitionHdfsPath(partitions: List[(String, Any)]): String = {
    partitions.map(prtn => s"${prtn._1}=${prtn._2}").mkString("/")
  }

  def saveData(rdd: RDD[Map[String, Any]], ms: Long): Unit = {
    val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
    if (newCacheLocked) {
      try {
        val ptns = getPartition(ms)
        val ptnsSql = ptns.map(ptn => (s"`${ptn._1}`=${ptn._2}")).mkString(", ")
        val ptnsPath = genPartitionHdfsPath(ptns)
        val filePath = s"${tablePath}/${ptnsPath}/${ms}"

        // encode data
        val dataRdd = rdd.map(encode(_))

        // save data to hdfs
        // fixme: waiting...

        // add partition info

        // submit ms
        submitCacheTime(ms)
        submitReadyTime(ms)
      } finally {
        newCacheLock.unlock()
      }
    }
    //    if (newDataFrame == null) {
    //      newDataFrame = df
    //      newDataFrame.persist(StorageLevel.fromString(cacheLevel))
    //    } else {
    //      if (!df.rdd.isEmpty) {
    //        newDataFrame.unpersist()
    //        newDataFrame = newDataFrame.unionAll(df)
    //        newDataFrame.persist(StorageLevel.fromString(cacheLevel))
    //      }
    //    }
    //    // submit ms
    //    submitCacheTime(ms)
    //    submitReadyTime(ms)
  }

  def readData(): Try[RDD[Map[String, Any]]] = Try {
    //    if (initialed) {
    //      val timeRange = TimeInfoCache.getTimeRange
    //      println(s"timeRange: ${timeRange}")
    //      submitLastProcTime(timeRange._2)
    //
    //      val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
    //      println(s"reviseTimeRange: ${reviseTimeRange}")
    //      dataFrame.filter(s"${timeStampColumn} BETWEEN ${reviseTimeRange._1} AND ${reviseTimeRange._2}")
    //    } else {
    //      throw new Exception("data not cached")
    //    }

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
      } finally {
        oldCacheLock.unlock()
      }
    } else {
      throw new Exception("old cache lock unavailable")
    }

    if (oldTempDataFrame == null && newTempDataFrame == null) {
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

  override def updateOldData(oldRdd: RDD[Map[String, Any]]): Unit = {
    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
    if (oldCacheLocked) {
      try {
        if (oldDataFrame == null) {
          oldDataFrame = oldDf
          oldDataFrame.persist(StorageLevel.fromString(cacheLevel))
        } else {
          if (!oldDf.rdd.isEmpty) {
            oldDataFrame.unpersist()
            oldDataFrame = oldDf
            oldDataFrame.persist(StorageLevel.fromString(cacheLevel))
          }
        }
      } finally {
        oldCacheLock.unlock()
      }
    }
  }

}
