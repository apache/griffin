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
package org.apache.griffin.measure.data.source.cache

import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.cache.tmst.TmstCache
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.temp.TimeRange
import org.apache.griffin.measure.rule.adaptor.InternalColumns
import org.apache.griffin.measure.utils.{HdfsUtil, TimeUtil}
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.griffin.measure.utils.DataFrameUtil._

import scala.util.Random

// data source cache process steps
// dump phase: save
// process phase: read -> process -> update -> finish -> clean old data
trait DataSourceCache extends DataCacheable with WithFanIn[Long] with Loggable with Serializable {

  val sqlContext: SQLContext
  val param: Map[String, Any]
  val dsName: String
  val index: Int

  var tmstCache: TmstCache = _
  protected def fromUntilRangeTmsts(from: Long, until: Long) = tmstCache.range(from, until)
  protected def clearTmst(t: Long) = tmstCache.remove(t)
  protected def clearTmstsUntil(until: Long) = {
    val outDateTmsts = tmstCache.until(until)
    tmstCache.remove(outDateTmsts)
  }
  protected def afterTilRangeTmsts(after: Long, til: Long) = fromUntilRangeTmsts(after + 1, til + 1)
  protected def clearTmstsTil(til: Long) = clearTmstsUntil(til + 1)

  val _FilePath = "file.path"
  val _InfoPath = "info.path"
  val _ReadyTimeInterval = "ready.time.interval"
  val _ReadyTimeDelay = "ready.time.delay"
  val _TimeRange = "time.range"

  val rdmStr = Random.alphanumeric.take(10).mkString
  val defFilePath = s"hdfs:///griffin/cache/${dsName}_${rdmStr}"
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

  val _ReadOnly = "read.only"
  val readOnly = param.getBoolean(_ReadOnly, false)

  val _Updatable = "updatable"
  val updatable = param.getBoolean(_Updatable, false)

//  val rowSepLiteral = "\n"
//  val partitionUnits: List[String] = List("hour", "min", "sec")
//  val minUnitTime: Long = TimeUtil.timeFromUnit(1, partitionUnits.last)

  val newCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.new")
  val oldCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.old")

  val newFilePath = s"${filePath}/new"
  val oldFilePath = s"${filePath}/old"

  val defOldCacheIndex = 0L

  protected def writeDataFrame(dfw: DataFrameWriter[Row], path: String): Unit
  protected def readDataFrame(dfr: DataFrameReader, path: String): DataFrame

  def init(): Unit = {}

  // save new cache data only, need index for multiple streaming data connectors
  def saveData(dfOpt: Option[DataFrame], ms: Long): Unit = {
    if (!readOnly) {
      dfOpt match {
        case Some(df) => {
          df.cache

          // cache df
          val cnt = df.count
          println(s"save ${dsName} data count: ${cnt}")

          // lock makes it safer when writing new cache data
          val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
          if (newCacheLocked) {
            try {
              val dfw = df.write.mode(SaveMode.Append).partitionBy(InternalColumns.tmst)
              writeDataFrame(dfw, newFilePath)
            } catch {
              case e: Throwable => error(s"save data error: ${e.getMessage}")
            } finally {
              newCacheLock.unlock()
            }
          }

          // uncache
          df.unpersist
        }
        case _ => {
          info(s"no data frame to save")
        }
      }

      // submit cache time and ready time
      if (fanIncrement(ms)) {
        println(s"save data [${ms}] finish")
        submitCacheTime(ms)
        submitReadyTime(ms)
      }

    }
  }

  // read new cache data and old cache data
  def readData(): (Option[DataFrame], TimeRange) = {
    // time range: (a, b]
    val timeRange = TimeInfoCache.getTimeRange
    val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)

    // read partition info
    val filterStr = if (reviseTimeRange._1 == reviseTimeRange._2) {
      println(s"read time range: [${reviseTimeRange._1}]")
      s"`${InternalColumns.tmst}` = ${reviseTimeRange._1}"
    } else {
      println(s"read time range: (${reviseTimeRange._1}, ${reviseTimeRange._2}]")
      s"`${InternalColumns.tmst}` > ${reviseTimeRange._1} AND `${InternalColumns.tmst}` <= ${reviseTimeRange._2}"
    }

    // new cache data
    val newDfOpt = try {
      val dfr = sqlContext.read
      Some(readDataFrame(dfr, newFilePath).filter(filterStr))
    } catch {
      case e: Throwable => {
        warn(s"read data source cache warn: ${e.getMessage}")
        None
      }
    }

    // old cache data
    val oldCacheIndexOpt = if (updatable) readOldCacheIndex else None
    val oldDfOpt = oldCacheIndexOpt.flatMap { idx =>
      val oldDfPath = s"${oldFilePath}/${idx}"
      try {
        val dfr = sqlContext.read
        Some(readDataFrame(dfr, oldDfPath).filter(filterStr))
      } catch {
        case e: Throwable => {
          warn(s"read old data source cache warn: ${e.getMessage}")
          None
        }
      }
    }

    // whole cache data
    val cacheDfOpt = unionDfOpts(newDfOpt, oldDfOpt)

    // from until tmst range
    val (from, until) = (reviseTimeRange._1, reviseTimeRange._2)
    val tmstSet = afterTilRangeTmsts(from, until)

    val retTimeRange = TimeRange(reviseTimeRange, tmstSet)
    (cacheDfOpt, retTimeRange)
  }

//  private def unionDfOpts(dfOpt1: Option[DataFrame], dfOpt2: Option[DataFrame]
//                         ): Option[DataFrame] = {
//    (dfOpt1, dfOpt2) match {
//      case (Some(df1), Some(df2)) => Some(unionByName(df1, df2))
//      case (Some(df1), _) => dfOpt1
//      case (_, Some(df2)) => dfOpt2
//      case _ => None
//    }
//  }
//
//  private def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
//    val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
//    a.select(columns: _*).unionAll(b.select(columns: _*))
//  }

  private def cleanOutTimePartitions(path: String, outTime: Long, partitionOpt: Option[String],
                                     func: (Long, Long) => Boolean
                                    ): Unit = {
    val earlierOrEqPaths = listPartitionsByFunc(path: String, outTime, partitionOpt, func)
    // delete out time data path
    earlierOrEqPaths.foreach { path =>
      println(s"delete hdfs path: ${path}")
      HdfsUtil.deleteHdfsPath(path)
    }
  }
  private def listPartitionsByFunc(path: String, bound: Long, partitionOpt: Option[String],
                                        func: (Long, Long) => Boolean
                                       ): Iterable[String] = {
    val names = HdfsUtil.listSubPathsByType(path, "dir")
    val regex = partitionOpt match {
      case Some(partition) => s"^${partition}=(\\d+)$$".r
      case _ => "^(\\d+)$".r
    }
    names.filter { name =>
      name match {
        case regex(value) => {
          str2Long(value) match {
            case Some(t) => func(t, bound)
            case _ => false
          }
        }
        case _ => false
      }
    }.map(name => s"${path}/${name}")
  }
  private def str2Long(str: String): Option[Long] = {
    try {
      Some(str.toLong)
    } catch {
      case e: Throwable => None
    }
  }

  // clean out time from new cache data and old cache data
  def cleanOutTimeData(): Unit = {
    // clean tmst
    val cleanTime = readCleanTime
    cleanTime.foreach(clearTmstsTil(_))

    if (!readOnly) {
      // new cache data
      val newCacheCleanTime = if (updatable) readLastProcTime else readCleanTime
      newCacheCleanTime match {
        case Some(nct) => {
          // clean calculated new cache data
          val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
          if (newCacheLocked) {
            try {
              cleanOutTimePartitions(newFilePath, nct, Some(InternalColumns.tmst),
                (a: Long, b: Long) => (a <= b))
            } catch {
              case e: Throwable => error(s"clean new cache data error: ${e.getMessage}")
            } finally {
              newCacheLock.unlock()
            }
          }
        }
        case _ => {
          // do nothing
        }
      }

      // old cache data
      val oldCacheCleanTime = if (updatable) readCleanTime else None
      oldCacheCleanTime match {
        case Some(oct) => {
          val oldCacheIndexOpt = readOldCacheIndex
          oldCacheIndexOpt.foreach { idx =>
            val oldDfPath = s"${oldFilePath}/${idx}"
            val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
            if (oldCacheLocked) {
              try {
                // clean calculated old cache data
                cleanOutTimePartitions(oldFilePath, idx, None, (a: Long, b: Long) => (a < b))
                // clean out time old cache data not calculated
//                cleanOutTimePartitions(oldDfPath, oct, Some(InternalColumns.tmst))
              } catch {
                case e: Throwable => error(s"clean old cache data error: ${e.getMessage}")
              } finally {
                oldCacheLock.unlock()
              }
            }
          }
        }
        case _ => {
          // do nothing
        }
      }
    }
  }

  // update old cache data
  def updateData(dfOpt: Option[DataFrame]): Unit = {
    if (!readOnly && updatable) {
      dfOpt match {
        case Some(df) => {
          // old cache lock
          val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
          if (oldCacheLocked) {
            try {
              val oldCacheIndexOpt = readOldCacheIndex
              val nextOldCacheIndex = oldCacheIndexOpt.getOrElse(defOldCacheIndex) + 1

              val oldDfPath = s"${oldFilePath}/${nextOldCacheIndex}"
//              val cleanTime = readCleanTime
//              val updateDf = cleanTime match {
//                case Some(ct) => {
//                  val filterStr = s"`${InternalColumns.tmst}` > ${ct}"
//                  df.filter(filterStr)
//                }
//                case _ => df
//              }
              val cleanTime = getNextCleanTime
              val filterStr = s"`${InternalColumns.tmst}` > ${cleanTime}"
              val updateDf = df.filter(filterStr)

              val prlCount = sqlContext.sparkContext.defaultParallelism
              // coalesce
//              val ptnCount = updateDf.rdd.getNumPartitions
//              val repartitionedDf = if (prlCount < ptnCount) {
//                updateDf.coalesce(prlCount)
//              } else updateDf
              // repartition
              val repartitionedDf = updateDf.repartition(prlCount)
              val dfw = repartitionedDf.write.mode(SaveMode.Overwrite)
              writeDataFrame(dfw, oldDfPath)

              submitOldCacheIndex(nextOldCacheIndex)
            } catch {
              case e: Throwable => error(s"update data error: ${e.getMessage}")
            } finally {
              oldCacheLock.unlock()
            }
          }
        }
        case _ => {
          info(s"no data frame to update")
        }
      }
    }
  }

  // process finish
  def processFinish(): Unit = {
    // next last proc time
    val timeRange = TimeInfoCache.getTimeRange
    submitLastProcTime(timeRange._2)

    // next clean time
    val nextCleanTime = timeRange._2 + deltaTimeRange._1
    submitCleanTime(nextCleanTime)
  }

  // read next clean time
  private def getNextCleanTime(): Long = {
    val timeRange = TimeInfoCache.getTimeRange
    val nextCleanTime = timeRange._2 + deltaTimeRange._1
    nextCleanTime
  }

}
