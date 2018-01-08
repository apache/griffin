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
package org.apache.griffin.measure.persist

import java.util.Date

import org.apache.griffin.measure.result._
import org.apache.griffin.measure.utils.{HdfsUtil, JsonUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Try
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.spark.TaskContext

// persist result and data to hdfs
case class HdfsPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  val Path = "path"
  val MaxPersistLines = "max.persist.lines"
  val MaxLinesPerFile = "max.lines.per.file"

  val path = config.getOrElse(Path, "").toString
  val maxPersistLines = config.getInt(MaxPersistLines, -1)
  val maxLinesPerFile = math.min(config.getInt(MaxLinesPerFile, 10000), 1000000)

  val separator = "/"

  val StartFile = filePath("_START")
  val FinishFile = filePath("_FINISH")
  val MetricsFile = filePath("_METRICS")

//  val MissRecFile = filePath("_MISSREC")      // optional
//  val MatchRecFile = filePath("_MATCHREC")    // optional

  val LogFile = filePath("_LOG")

  val _MetricName = "metricName"
  val _Timestamp = "timestamp"
  val _Value = "value"

  var _init = true
  private def isInit = {
    val i = _init
    _init = false
    i
  }

  def available(): Boolean = {
    path.nonEmpty
  }

  private def persistHead: String = {
    val dt = new Date(timeStamp)
    s"================ log of ${dt} ================\n"
  }

  private def timeHead(rt: Long): String = {
    val dt = new Date(rt)
    s"--- ${dt} ---\n"
  }

  protected def filePath(file: String): String = {
    HdfsUtil.getHdfsFilePath(path, s"${metricName}/${timeStamp}/${file}")
  }

  protected def withSuffix(path: String, suffix: String): String = {
    s"${path}.${suffix}"
  }

  def start(msg: String): Unit = {
    try {
      HdfsUtil.writeContent(StartFile, msg)
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }
  def finish(): Unit = {
    try {
      HdfsUtil.createEmptyFile(FinishFile)
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

//  def result(rt: Long, result: Result): Unit = {
//    try {
//      val resStr = result match {
//        case ar: AccuracyResult => {
//          s"match percentage: ${ar.matchPercentage}\ntotal count: ${ar.getTotal}\nmiss count: ${ar.getMiss}, match count: ${ar.getMatch}"
//        }
//        case pr: ProfileResult => {
//          s"match percentage: ${pr.matchPercentage}\ntotal count: ${pr.getTotal}\nmiss count: ${pr.getMiss}, match count: ${pr.getMatch}"
//        }
//        case _ => {
//          s"result: ${result}"
//        }
//      }
//      HdfsUtil.writeContent(ResultFile, timeHead(rt) + resStr)
//      log(rt, resStr)
//
//      info(resStr)
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }

  // need to avoid string too long
//  private def rddRecords(records: RDD[String], path: String): Unit = {
//    try {
//      val recordCount = records.count
//      val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
//      if (count > 0) {
//        val groupCount = ((count - 1) / maxLinesPerFile + 1).toInt
//        if (groupCount <= 1) {
//          val recs = records.take(count.toInt)
//          persistRecords(path, recs)
//        } else {
//          val groupedRecords: RDD[(Long, Iterable[String])] =
//            records.zipWithIndex.flatMap { r =>
//              val gid = r._2 / maxLinesPerFile
//              if (gid < groupCount) Some((gid, r._1)) else None
//            }.groupByKey()
//          groupedRecords.foreach { group =>
//            val (gid, recs) = group
//            val hdfsPath = if (gid == 0) path else withSuffix(path, gid.toString)
//            persistRecords(hdfsPath, recs)
//          }
//        }
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }
//
//  private def iterableRecords(records: Iterable[String], path: String): Unit = {
//    try {
//      val recordCount = records.size
//      val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
//      if (count > 0) {
//        val groupCount = ((count - 1) / maxLinesPerFile + 1).toInt
//        if (groupCount <= 1) {
//          val recs = records.take(count.toInt)
//          persistRecords(path, recs)
//        } else {
//          val groupedRecords = records.grouped(groupCount).zipWithIndex
//          groupedRecords.take(groupCount).foreach { group =>
//            val (recs, gid) = group
//            val hdfsPath = if (gid == 0) path else withSuffix(path, gid.toString)
//            persistRecords(hdfsPath, recs)
//          }
//        }
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }
//
//  def records(recs: RDD[String], tp: String): Unit = {
//    tp match {
//      case PersistDataType.MISS => rddRecords(recs, MissRecFile)
//      case PersistDataType.MATCH => rddRecords(recs, MatchRecFile)
//      case _ => {}
//    }
//  }
//
//  def records(recs: Iterable[String], tp: String): Unit = {
//    tp match {
//      case PersistDataType.MISS => iterableRecords(recs, MissRecFile)
//      case PersistDataType.MATCH => iterableRecords(recs, MatchRecFile)
//      case _ => {}
//    }
//  }

//  private def persistRecords2Hdfs(hdfsPath: String, rdd: RDD[String]): Unit = {
//    try {
////      rdd.saveAsTextFile(hdfsPath)
//      val recStr = rdd.collect().mkString("\n")
//      HdfsUtil.writeContent(hdfsPath, recStr)
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }
  private def persistRecords2Hdfs(hdfsPath: String, records: Iterable[String]): Unit = {
    try {
      val recStr = records.mkString("\n")
      HdfsUtil.writeContent(hdfsPath, recStr)
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  def log(rt: Long, msg: String): Unit = {
    try {
      val logStr = (if (isInit) persistHead else "") + timeHead(rt) + s"${msg}\n\n"
      HdfsUtil.appendContent(LogFile, logStr)
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  private def getHdfsPath(path: String, groupId: Int): String = {
    HdfsUtil.getHdfsFilePath(path, s"${groupId}")
//    if (groupId == 0) path else withSuffix(path, s"${groupId}")
  }
  private def getHdfsPath(path: String, ptnId: Int, groupId: Int): String = {
    HdfsUtil.getHdfsFilePath(path, s"${ptnId}.${groupId}")
//    if (ptnId == 0 && groupId == 0) path else withSuffix(path, s"${ptnId}.${groupId}")
  }

  private def clearOldRecords(path: String): Unit = {
    HdfsUtil.deleteHdfsPath(path)
  }

  def persistRecords(df: DataFrame, name: String): Unit = {
    val path = filePath(name)
    clearOldRecords(path)
    try {
      val recordCount = df.count
      val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
      val maxCount = count.toInt
      if (maxCount > 0) {
        val recDf = df.limit(maxCount)
        recDf.toJSON.foreachPartition { ptn =>
          val ptnid = TaskContext.getPartitionId()
          val groupedRecords = ptn.grouped(maxLinesPerFile).zipWithIndex
          groupedRecords.foreach { group =>
            val (recs, gid) = group
            val hdfsPath = getHdfsPath(path, ptnid, gid)
            persistRecords2Hdfs(hdfsPath, recs)
          }
        }
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  def persistRecords(records: RDD[String], name: String): Unit = {
    val path = filePath(name)
    clearOldRecords(path)
    try {
      val recordCount = records.count
      val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
      if (count > 0) {
        val groupCount = ((count - 1) / maxLinesPerFile + 1).toInt
        if (groupCount <= 1) {
          val recs = records.take(count.toInt)
          persistRecords2Hdfs(path, recs)
        } else {
          val groupedRecords: RDD[(Long, Iterable[String])] =
            records.zipWithIndex.flatMap { r =>
              val gid = r._2 / maxLinesPerFile
              if (gid < groupCount) Some((gid, r._1)) else None
            }.groupByKey()
          groupedRecords.foreach { group =>
            val (gid, recs) = group
            val hdfsPath = if (gid == 0) path else withSuffix(path, gid.toString)
            persistRecords2Hdfs(hdfsPath, recs)
          }
        }
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  def persistRecords(records: Iterable[String], name: String): Unit = {
    val path = filePath(name)
    clearOldRecords(path)
    try {
      val recordCount = records.size
      val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
      if (count > 0) {
        val groupCount = (count - 1) / maxLinesPerFile + 1
        if (groupCount <= 1) {
          val recs = records.take(count.toInt)
          persistRecords2Hdfs(path, recs)
        } else {
          val groupedRecords = records.grouped(maxLinesPerFile).zipWithIndex
          groupedRecords.take(groupCount).foreach { group =>
            val (recs, gid) = group
            val hdfsPath = getHdfsPath(path, gid)
            persistRecords2Hdfs(hdfsPath, recs)
          }
        }
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

//  def persistMetrics(metrics: Seq[String], name: String): Unit = {
//    val path = filePath(name)
//    try {
//      val recordCount = metrics.size
//      val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
//      if (count > 0) {
//        val groupCount = ((count - 1) / maxLinesPerFile + 1).toInt
//        if (groupCount <= 1) {
//          val recs = metrics.take(count.toInt)
//          persistRecords(path, recs)
//        } else {
//          val groupedRecords = metrics.grouped(groupCount).zipWithIndex
//          groupedRecords.take(groupCount).foreach { group =>
//            val (recs, gid) = group
//            val hdfsPath = if (gid == 0) path else withSuffix(path, gid.toString)
//            persistRecords(hdfsPath, recs)
//          }
//        }
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }

  def persistMetrics(metrics: Map[String, Any]): Unit = {
    val head = Map[String, Any]((_MetricName -> metricName), (_Timestamp -> timeStamp))
    val result = head + (_Value -> metrics)
    try {
      val json = JsonUtil.toJson(result)
      persistRecords2Hdfs(MetricsFile, json :: Nil)
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

}
