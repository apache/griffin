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
package org.apache.griffin.measure.sink

import java.util.Date

import org.apache.spark.rdd.RDD

import org.apache.griffin.measure.utils.{HdfsUtil, JsonUtil}
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * sink metric and record to hdfs
  */
case class HdfsSink(config: Map[String, Any], metricName: String, timeStamp: Long) extends Sink {

  val block: Boolean = true

  val Path = "path"
  val MaxPersistLines = "max.persist.lines"
  val MaxLinesPerFile = "max.lines.per.file"

  val path = config.getOrElse(Path, "").toString
  val maxPersistLines = config.getInt(MaxPersistLines, -1)
  val maxLinesPerFile = math.min(config.getInt(MaxLinesPerFile, 10000), 1000000)

  val StartFile = filePath("_START")
  val FinishFile = filePath("_FINISH")
  val MetricsFile = filePath("_METRICS")

  val LogFile = filePath("_LOG")

  var _init = true

  def available(): Boolean = {
    path.nonEmpty
  }

  private def logHead: String = {
    if (_init) {
      _init = false
      val dt = new Date(timeStamp)
      s"================ log of ${dt} ================\n"
    } else ""
  }

  private def timeHead(rt: Long): String = {
    val dt = new Date(rt)
    s"--- ${dt} ---\n"
  }

  private def logWrap(rt: Long, msg: String): String = {
    logHead + timeHead(rt) + s"${msg}\n\n"
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
      case e: Throwable => error(e.getMessage, e)
    }
  }

  def finish(): Unit = {
    try {
      HdfsUtil.createEmptyFile(FinishFile)
    } catch {
      case e: Throwable => error(e.getMessage, e)
    }
  }

  def log(rt: Long, msg: String): Unit = {
    try {
      val logStr = logWrap(rt, msg)
      HdfsUtil.appendContent(LogFile, logStr)
    } catch {
      case e: Throwable => error(e.getMessage, e)
    }
  }

  private def getHdfsPath(path: String, groupId: Int): String = {
    HdfsUtil.getHdfsFilePath(path, s"${groupId}")
  }

  private def getHdfsPath(path: String, ptnId: Int, groupId: Int): String = {
    HdfsUtil.getHdfsFilePath(path, s"${ptnId}.${groupId}")
  }

  private def clearOldRecords(path: String): Unit = {
    HdfsUtil.deleteHdfsPath(path)
  }

  def sinkRecords(records: RDD[String], name: String): Unit = {
    val path = filePath(name)
    clearOldRecords(path)
    try {
      val recordCount = records.count

      val count =
        if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)

      if (count > 0) {
        val groupCount = ((count - 1) / maxLinesPerFile + 1).toInt
        if (groupCount <= 1) {
          val recs = records.take(count.toInt)
          sinkRecords2Hdfs(path, recs)
        } else {
          val groupedRecords: RDD[(Long, Iterable[String])] =
            records.zipWithIndex.flatMap { r =>
              val gid = r._2 / maxLinesPerFile
              if (gid < groupCount) Some((gid, r._1)) else None
            }.groupByKey()
          groupedRecords.foreach { group =>
            val (gid, recs) = group
            val hdfsPath = if (gid == 0) path else withSuffix(path, gid.toString)
            sinkRecords2Hdfs(hdfsPath, recs)
          }
        }
      }
    } catch {
      case e: Throwable => error(e.getMessage, e)
    }
  }

  def sinkRecords(records: Iterable[String], name: String): Unit = {
    val path = filePath(name)
    clearOldRecords(path)
    try {
      val recordCount = records.size

      val count =
        if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)

      if (count > 0) {
        val groupCount = (count - 1) / maxLinesPerFile + 1
        if (groupCount <= 1) {
          val recs = records.take(count.toInt)
          sinkRecords2Hdfs(path, recs)
        } else {
          val groupedRecords = records.grouped(maxLinesPerFile).zipWithIndex
          groupedRecords.take(groupCount).foreach { group =>
            val (recs, gid) = group
            val hdfsPath = getHdfsPath(path, gid)
            sinkRecords2Hdfs(hdfsPath, recs)
          }
        }
      }
    } catch {
      case e: Throwable => error(e.getMessage, e)
    }
  }

  def sinkMetrics(metrics: Map[String, Any]): Unit = {
    try {
      val json = JsonUtil.toJson(metrics)
      sinkRecords2Hdfs(MetricsFile, json :: Nil)
    } catch {
      case e: Throwable => error(e.getMessage, e)
    }
  }

  private def sinkRecords2Hdfs(hdfsPath: String, records: Iterable[String]): Unit = {
    try {
      val recStr = records.mkString("\n")
      HdfsUtil.writeContent(hdfsPath, recStr)
    } catch {
      case e: Throwable => error(e.getMessage, e)
    }
  }

}
