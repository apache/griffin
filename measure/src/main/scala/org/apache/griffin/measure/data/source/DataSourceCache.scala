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
package org.apache.griffin.measure.data.source

import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.data.connector.streaming.StreamingDataConnector
import org.apache.griffin.measure.data.connector._
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.utils.{HdfsFileDumpUtil, HdfsUtil, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success}
import org.apache.griffin.measure.utils.ParamUtil._

case class DataSourceCache(sqlContext: SQLContext, param: Map[String, Any],
                           metricName: String, index: Int
                          ) extends DataCacheable with Loggable with Serializable {

  val name = ""

  val _FilePath = "file.path"
  val _InfoPath = "info.path"
  val _ReadyTimeInterval = "ready.time.interval"
  val _ReadyTimeDelay = "ready.time.delay"
  val _TimeRange = "time.range"

  val defFilePath = s"hdfs:///griffin/cache/${metricName}/${index}"
  val defInfoPath = s"${index}"

  val filePath: String = param.getString(_FilePath, defFilePath)
  val cacheInfoPath: String = param.getString(_InfoPath, defInfoPath)
  val readyTimeInterval: Long = TimeUtil.milliseconds(param.getString(_ReadyTimeInterval, "1m")).getOrElse(60000L)
  val readyTimeDelay: Long = TimeUtil.milliseconds(param.getString(_ReadyTimeDelay, "1m")).getOrElse(60000L)
  val deltaTimeRange: (Long, Long) = {
    def negative(n: Long): Long = if (n <= 0) n else 0
    param.get(_TimeRange) match {
      case Some(seq: Seq[String]) => {
        val nseq = seq.flatMap(TimeUtil.milliseconds(_))
        val ns = negative(nseq.headOption.getOrElse(0))
        val ne = negative(nseq.tail.headOption.getOrElse(0))
        (ns, ne)
      }
      case _ => (0, 0)
    }
  }

  val rowSepLiteral = "\n"
  val partitionUnits: List[String] = List("hour", "min", "sec")

  val newCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.new")
  val oldCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.old")

  def init(): Unit = {
    ;
  }

  def saveData(dfOpt: Option[DataFrame], ms: Long): Unit = {
    dfOpt match {
      case Some(df) => {
        val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
        if (newCacheLocked) {
          try {
            val ptns = getPartition(ms)
            val ptnsPath = genPartitionHdfsPath(ptns)
            val dirPath = s"${filePath}/${ptnsPath}"
            val dataFileName = s"${ms}"
            val dataFilePath = HdfsUtil.getHdfsFilePath(dirPath, dataFileName)

            // transform data
            val dataRdd: RDD[String] = df.toJSON

            // save data
            val dumped = if (!dataRdd.isEmpty) {
              HdfsFileDumpUtil.dump(dataFilePath, dataRdd, rowSepLiteral)
            } else false

          } catch {
            case e: Throwable => error(s"save data error: ${e.getMessage}")
          } finally {
            newCacheLock.unlock()
          }
        }
      }
      case _ => {
        info(s"no data frame to save")
      }
    }

    // submit cache time and ready time
    submitCacheTime(ms)
    submitReadyTime(ms)
  }

  def readData(): Option[DataFrame] = {
    val timeRange = TimeInfoCache.getTimeRange
    submitLastProcTime(timeRange._2)

    val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
    submitCleanTime(reviseTimeRange._1)

    // read directly through partition info
    val partitionRanges = getPartitionRange(reviseTimeRange._1, reviseTimeRange._2)
    println(s"read time ranges: ${reviseTimeRange}")
    println(s"read partition ranges: ${partitionRanges}")

    // list partition paths
    val partitionPaths = listPathsBetweenRanges(filePath :: Nil, partitionRanges)

    if (partitionPaths.isEmpty) {
      None
    } else {
      try {
        Some(sqlContext.read.json(partitionPaths: _*))
      } catch {
        case e: Throwable => {
          warn(s"read data source cache warn: ${e.getMessage}")
          None
        }
      }
    }
  }

  // -- deprecated --
  def updateData(df: DataFrame, ms: Long): Unit = {
    val ptns = getPartition(ms)
    val ptnsPath = genPartitionHdfsPath(ptns)
    val dirPath = s"${filePath}/${ptnsPath}"
    val dataFileName = s"${ms}"
    val dataFilePath = HdfsUtil.getHdfsFilePath(dirPath, dataFileName)

    try {
      val records = df.toJSON
      val arr = records.collect
      val needSave = !arr.isEmpty

      // remove out time old data
      HdfsFileDumpUtil.remove(dirPath, dataFileName, true)
      println(s"remove file path: ${dirPath}/${dataFileName}")

      // save updated data
      val dumped = if (needSave) {
        HdfsFileDumpUtil.dump(dataFilePath, arr, rowSepLiteral)
        println(s"update file path: ${dataFilePath}")
      } else false
    } catch {
      case e: Throwable => error(s"update data error: ${e.getMessage}")
    }
  }

  def updateData(rdd: RDD[String], ms: Long, cnt: Long): Unit = {
    val ptns = getPartition(ms)
    val ptnsPath = genPartitionHdfsPath(ptns)
    val dirPath = s"${filePath}/${ptnsPath}"
    val dataFileName = s"${ms}"
    val dataFilePath = HdfsUtil.getHdfsFilePath(dirPath, dataFileName)

    try {
//      val needSave = !rdd.isEmpty

      // remove out time old data
      HdfsFileDumpUtil.remove(dirPath, dataFileName, true)
      println(s"remove file path: ${dirPath}/${dataFileName}")

      // save updated data
      val dumped = if (cnt > 0) {
        HdfsFileDumpUtil.dump(dataFilePath, rdd, rowSepLiteral)
        println(s"update file path: ${dataFilePath}")
      } else false
    } catch {
      case e: Throwable => error(s"update data error: ${e.getMessage}")
    } finally {
      rdd.unpersist()
    }
  }

  def updateData(rdd: Iterable[String], ms: Long): Unit = {
    val ptns = getPartition(ms)
    val ptnsPath = genPartitionHdfsPath(ptns)
    val dirPath = s"${filePath}/${ptnsPath}"
    val dataFileName = s"${ms}"
    val dataFilePath = HdfsUtil.getHdfsFilePath(dirPath, dataFileName)

    try {
      val needSave = !rdd.isEmpty

      // remove out time old data
      HdfsFileDumpUtil.remove(dirPath, dataFileName, true)
      println(s"remove file path: ${dirPath}/${dataFileName}")

      // save updated data
      val dumped = if (needSave) {
        HdfsFileDumpUtil.dump(dataFilePath, rdd, rowSepLiteral)
        println(s"update file path: ${dataFilePath}")
      } else false
    } catch {
      case e: Throwable => error(s"update data error: ${e.getMessage}")
    }
  }

  def updateDataMap(dfMap: Map[Long, DataFrame]): Unit = {
    val dataMap = dfMap.map { pair =>
      val (t, recs) = pair
      val rdd = recs.toJSON
//      rdd.cache
      (t, rdd, rdd.count)
    }

    dataMap.foreach { pair =>
      val (t, arr, cnt) = pair
      updateData(arr, t, cnt)
    }
  }

  def cleanOldData(): Unit = {
    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
    if (oldCacheLocked) {
      try {
        val cleanTime = readCleanTime()
        cleanTime match {
          case Some(ct) => {
            // drop partitions
            val bounds = getPartition(ct)

            // list partition paths
            val earlierPaths = listPathsEarlierThanBounds(filePath :: Nil, bounds)

            // delete out time data path
            earlierPaths.foreach { path =>
              println(s"delete hdfs path: ${path}")
              HdfsUtil.deleteHdfsPath(path)
            }
          }
          case _ => {
            // do nothing
          }
        }
      } catch {
        case e: Throwable => error(s"clean old data error: ${e.getMessage}")
      } finally {
        oldCacheLock.unlock()
      }
    }
  }

  override protected def genCleanTime(ms: Long): Long = {
    val minPartitionUnit = partitionUnits.last
    val t1 = TimeUtil.timeToUnit(ms, minPartitionUnit)
    val t2 = TimeUtil.timeFromUnit(t1, minPartitionUnit)
    t2
  }

  private def getPartition(ms: Long): List[Long] = {
    partitionUnits.map { unit =>
      TimeUtil.timeToUnit(ms, unit)
    }
  }
  private def getPartitionRange(ms1: Long, ms2: Long): List[(Long, Long)] = {
    partitionUnits.map { unit =>
      val t1 = TimeUtil.timeToUnit(ms1, unit)
      val t2 = TimeUtil.timeToUnit(ms2, unit)
      (t1, t2)
    }
  }
  private def genPartitionHdfsPath(partition: List[Long]): String = {
    partition.map(prtn => s"${prtn}").mkString("/")
  }
  private def str2Long(str: String): Option[Long] = {
    try {
      Some(str.toLong)
    } catch {
      case e: Throwable => None
    }
  }


  // here the range means [min, max], but the best range should be (min, max]
  private def listPathsBetweenRanges(paths: List[String],
                                     partitionRanges: List[(Long, Long)]
                                    ): List[String] = {
    partitionRanges match {
      case Nil => paths
      case head :: tail => {
        val (lb, ub) = head
        val curPaths = paths.flatMap { path =>
          val names = HdfsUtil.listSubPathsByType(path, "dir").toList
          names.filter { name =>
            str2Long(name) match {
              case Some(t) => (t >= lb) && (t <= ub)
              case _ => false
            }
          }.map(HdfsUtil.getHdfsFilePath(path, _))
        }
        listPathsBetweenRanges(curPaths, tail)
      }
    }
  }
  private def listPathsEarlierThanBounds(paths: List[String], bounds: List[Long]
                                        ): List[String] = {
    bounds match {
      case Nil => paths
      case head :: tail => {
        val earlierPaths = paths.flatMap { path =>
          val names = HdfsUtil.listSubPathsByType(path, "dir").toList
          names.filter { name =>
            str2Long(name) match {
              case Some(t) => (t < head)
              case _ => false
            }
          }.map(HdfsUtil.getHdfsFilePath(path, _))
        }
        val equalPaths = paths.flatMap { path =>
          val names = HdfsUtil.listSubPathsByType(path, "dir").toList
          names.filter { name =>
            str2Long(name) match {
              case Some(t) => (t == head)
              case _ => false
            }
          }.map(HdfsUtil.getHdfsFilePath(path, _))
        }

        tail match {
          case Nil => earlierPaths
          case _ => earlierPaths ::: listPathsEarlierThanBounds(equalPaths, tail)
        }
      }
    }
  }
}
