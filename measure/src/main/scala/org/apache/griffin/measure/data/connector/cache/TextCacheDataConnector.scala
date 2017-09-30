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
//import java.util.concurrent.TimeUnit
//
//import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
//import org.apache.griffin.measure.config.params.user.DataCacheParam
//import org.apache.griffin.measure.result.TimeStampInfo
//import org.apache.griffin.measure.utils.{HdfsFileDumpUtil, HdfsUtil, JsonUtil, TimeUtil}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
//
//import scala.util.Try
//
//case class TextCacheDataConnector(sqlContext: SQLContext, dataCacheParam: DataCacheParam
//                                 ) extends CacheDataConnector {
//
//  val config = dataCacheParam.config
//  val InfoPath = "info.path"
//  val cacheInfoPath: String = config.getOrElse(InfoPath, defCacheInfoPath).toString
//
//  val newCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.new")
//  val oldCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.old")
//
//  val timeRangeParam: List[String] = if (dataCacheParam.timeRange != null) dataCacheParam.timeRange else Nil
//  val deltaTimeRange: (Long, Long) = (timeRangeParam ::: List("0", "0")) match {
//    case s :: e :: _ => {
//      val ns = TimeUtil.milliseconds(s) match {
//        case Some(n) if (n < 0) => n
//        case _ => 0
//      }
//      val ne = TimeUtil.milliseconds(e) match {
//        case Some(n) if (n < 0) => n
//        case _ => 0
//      }
//      (ns, ne)
//    }
//    case _ => (0, 0)
//  }
//
//  val FilePath = "file.path"
//  val filePath: String = config.get(FilePath) match {
//    case Some(s: String) => s
//    case _ => throw new Exception("invalid file.path!")
//  }
//
//  val ReadyTimeInterval = "ready.time.interval"
//  val ReadyTimeDelay = "ready.time.delay"
//  val readyTimeInterval: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeInterval, "1m").toString).getOrElse(60000L)
//  val readyTimeDelay: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeDelay, "1m").toString).getOrElse(60000L)
//
////  val TimeStampColumn: String = TimeStampInfo.key
////  val PayloadColumn: String = "payload"
//
//  // cache schema: Long, String
////  val fields = List[StructField](
////    StructField(TimeStampColumn, LongType),
////    StructField(PayloadColumn, StringType)
////  )
////  val schema = StructType(fields)
//
//  //  case class CacheData(time: Long, payload: String) {
//  //    def getTime(): Long = time
//  //    def getPayload(): String = payload
//  //  }
//
//  private val rowSepLiteral = "\n"
//
//  val partitionUnits: List[String] = List("hour", "min")
//
//  override def init(): Unit = {
//    // do nothing
//  }
//
//  def available(): Boolean = {
//    true
//  }
//
//  private def encode(data: Map[String, Any], ms: Long): Option[String] = {
//    try {
//      val map = data + (TimeStampInfo.key -> ms)
//      Some(JsonUtil.toJson(map))
//    } catch {
//      case _: Throwable => None
//    }
//  }
//
//  private def decode(data: String): Option[Map[String, Any]] = {
//    try {
//      Some(JsonUtil.toAnyMap(data))
//    } catch {
//      case _: Throwable => None
//    }
//  }
//
//  def saveData(rdd: RDD[Map[String, Any]], ms: Long): Unit = {
//    val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
//    if (newCacheLocked) {
//      try {
//        val ptns = getPartition(ms)
//        val ptnsPath = genPartitionHdfsPath(ptns)
//        val dirPath = s"${filePath}/${ptnsPath}"
//        val dataFileName = s"${ms}"
//        val dataFilePath = HdfsUtil.getHdfsFilePath(dirPath, dataFileName)
//
//        // encode data
//        val dataRdd: RDD[String] = rdd.flatMap(encode(_, ms))
//
//        // save data
//        val dumped = if (!dataRdd.isEmpty) {
//          HdfsFileDumpUtil.dump(dataFilePath, dataRdd, rowSepLiteral)
//        } else false
//
//        // submit ms
//        submitCacheTime(ms)
//        submitReadyTime(ms)
//      } catch {
//        case e: Throwable => error(s"save data error: ${e.getMessage}")
//      } finally {
//        newCacheLock.unlock()
//      }
//    }
//  }
//
//  def readData(): Try[RDD[Map[String, Any]]] = Try {
//    val timeRange = TimeInfoCache.getTimeRange
//    submitLastProcTime(timeRange._2)
//
//    val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
//    submitCleanTime(reviseTimeRange._1)
//
//    // read directly through partition info
//    val partitionRanges = getPartitionRange(reviseTimeRange._1, reviseTimeRange._2)
//    println(s"read time ranges: ${reviseTimeRange}")
//    println(s"read partition ranges: ${partitionRanges}")
//
//    // list partition paths
//    val partitionPaths = listPathsBetweenRanges(filePath :: Nil, partitionRanges)
//
//    if (partitionPaths.isEmpty) {
//      sqlContext.sparkContext.emptyRDD[Map[String, Any]]
//    } else {
//      val filePaths = partitionPaths.mkString(",")
//      val rdd = sqlContext.sparkContext.textFile(filePaths)
//
//      // decode data
//      rdd.flatMap { row =>
//        decode(row)
//      }
//    }
//  }
//
//  override def cleanOldData(): Unit = {
//    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
//    if (oldCacheLocked) {
//      try {
//        val cleanTime = readCleanTime()
//        cleanTime match {
//          case Some(ct) => {
//            // drop partitions
//            val bounds = getPartition(ct)
//
//            // list partition paths
//            val earlierPaths = listPathsEarlierThanBounds(filePath :: Nil, bounds)
//
//            // delete out time data path
//            earlierPaths.foreach { path =>
//              println(s"delete hdfs path: ${path}")
//              HdfsUtil.deleteHdfsPath(path)
//            }
//          }
//          case _ => {
//            // do nothing
//          }
//        }
//      } catch {
//        case e: Throwable => error(s"clean old data error: ${e.getMessage}")
//      } finally {
//        oldCacheLock.unlock()
//      }
//    }
//  }
//
//  override def updateOldData(t: Long, oldData: Iterable[Map[String, Any]]): Unit = {
//    // parallel process different time groups, lock is unnecessary
//    val ptns = getPartition(t)
//    val ptnsPath = genPartitionHdfsPath(ptns)
//    val dirPath = s"${filePath}/${ptnsPath}"
//    val dataFileName = s"${t}"
//    val dataFilePath = HdfsUtil.getHdfsFilePath(dirPath, dataFileName)
//
//    try {
//      // remove out time old data
//      HdfsFileDumpUtil.remove(dirPath, dataFileName, true)
//
//      // save updated old data
//      if (oldData.size > 0) {
//        val recordDatas = oldData.flatMap { dt =>
//          encode(dt, t)
//        }
//        val dumped = HdfsFileDumpUtil.dump(dataFilePath, recordDatas, rowSepLiteral)
//      }
//    } catch {
//      case e: Throwable => error(s"update old data error: ${e.getMessage}")
//    }
//  }
//
//  override protected def genCleanTime(ms: Long): Long = {
//    val minPartitionUnit = partitionUnits.last
//    val t1 = TimeUtil.timeToUnit(ms, minPartitionUnit)
//    val t2 = TimeUtil.timeFromUnit(t1, minPartitionUnit)
//    t2
//  }
//
//  private def getPartition(ms: Long): List[Long] = {
//    partitionUnits.map { unit =>
//      TimeUtil.timeToUnit(ms, unit)
//    }
//  }
//  private def getPartitionRange(ms1: Long, ms2: Long): List[(Long, Long)] = {
//    partitionUnits.map { unit =>
//      val t1 = TimeUtil.timeToUnit(ms1, unit)
//      val t2 = TimeUtil.timeToUnit(ms2, unit)
//      (t1, t2)
//    }
//  }
//
//  private def genPartitionHdfsPath(partition: List[Long]): String = {
//    partition.map(prtn => s"${prtn}").mkString("/")
//  }
//
//  private def str2Long(str: String): Option[Long] = {
//    try {
//      Some(str.toLong)
//    } catch {
//      case e: Throwable => None
//    }
//  }
//
//  // here the range means [min, max], but the best range should be (min, max]
//  private def listPathsBetweenRanges(paths: List[String],
//                                     partitionRanges: List[(Long, Long)]
//                                    ): List[String] = {
//    partitionRanges match {
//      case Nil => paths
//      case head :: tail => {
//        val (lb, ub) = head
//        val curPaths = paths.flatMap { path =>
//          val names = HdfsUtil.listSubPaths(path, "dir").toList
//          names.filter { name =>
//            str2Long(name) match {
//              case Some(t) => (t >= lb) && (t <= ub)
//              case _ => false
//            }
//          }.map(HdfsUtil.getHdfsFilePath(path, _))
//        }
//        listPathsBetweenRanges(curPaths, tail)
//      }
//    }
//  }
//
//  private def listPathsEarlierThanBounds(paths: List[String], bounds: List[Long]
//                                        ): List[String] = {
//    bounds match {
//      case Nil => paths
//      case head :: tail => {
//        val earlierPaths = paths.flatMap { path =>
//          val names = HdfsUtil.listSubPaths(path, "dir").toList
//          names.filter { name =>
//            str2Long(name) match {
//              case Some(t) => (t < head)
//              case _ => false
//            }
//          }.map(HdfsUtil.getHdfsFilePath(path, _))
//        }
//        val equalPaths = paths.flatMap { path =>
//          val names = HdfsUtil.listSubPaths(path, "dir").toList
//          names.filter { name =>
//            str2Long(name) match {
//              case Some(t) => (t == head)
//              case _ => false
//            }
//          }.map(HdfsUtil.getHdfsFilePath(path, _))
//        }
//
//        tail match {
//          case Nil => earlierPaths
//          case _ => earlierPaths ::: listPathsEarlierThanBounds(equalPaths, tail)
//        }
//      }
//    }
//  }
//
//}
