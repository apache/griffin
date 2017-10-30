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
import org.apache.griffin.measure.utils.ParamUtil._

// persist result and data to hdfs
case class LoggerPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  val MaxLogLines = "max.log.lines"

  val maxLogLines = config.getInt(MaxLogLines, 100)

  def available(): Boolean = true

  def start(msg: String): Unit = {
    println(s"[${timeStamp}] ${metricName} start: ${msg}")
  }
  def finish(): Unit = {
    println(s"[${timeStamp}] ${metricName} finish")
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
//      println(s"[${timeStamp}] ${metricName} result: \n${resStr}")
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }
//
//  // need to avoid string too long
//  private def rddRecords(records: RDD[String]): Unit = {
//    try {
//      val recordCount = records.count.toInt
//      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
//      if (count > 0) {
//        val recordsArray = records.take(count)
////        recordsArray.foreach(println)
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }

//  private def iterableRecords(records: Iterable[String]): Unit = {
//    try {
//      val recordCount = records.size
//      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
//      if (count > 0) {
//        val recordsArray = records.take(count)
////        recordsArray.foreach(println)
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }

//  def records(recs: RDD[String], tp: String): Unit = {
//    tp match {
//      case PersistDataType.MISS => rddRecords(recs)
//      case PersistDataType.MATCH => rddRecords(recs)
//      case _ => {}
//    }
//  }
//
//  def records(recs: Iterable[String], tp: String): Unit = {
//    tp match {
//      case PersistDataType.MISS => iterableRecords(recs)
//      case PersistDataType.MATCH => iterableRecords(recs)
//      case _ => {}
//    }
//  }

//  def missRecords(records: RDD[String]): Unit = {
//    warn(s"[${timeStamp}] ${metricName} miss records: ")
//    rddRecords(records)
//  }
//  def matchRecords(records: RDD[String]): Unit = {
//    warn(s"[${timeStamp}] ${metricName} match records: ")
//    rddRecords(records)
//  }

  def log(rt: Long, msg: String): Unit = {
    println(s"[${timeStamp}] ${rt}: ${msg}")
  }

//  def persistRecords(df: DataFrame, name: String): Unit = {
//    val records = df.toJSON
//    println(s"${name} [${timeStamp}] records: ")
//    try {
//      val recordCount = records.count.toInt
//      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
//      if (count > 0) {
//        val recordsArray = records.take(count)
//        recordsArray.foreach(println)
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }

  def persistRecords(records: Iterable[String], name: String): Unit = {
    try {
      val recordCount = records.size
      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
      if (count > 0) {
        records.foreach(println)
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

//  def persistMetrics(metrics: Seq[String], name: String): Unit = {
//    try {
//      val recordCount = metrics.size
//      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
//      if (count > 0) {
//        val recordsArray = metrics.take(count)
//        recordsArray.foreach(println)
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
//  }

  def persistMetrics(metrics: Map[String, Any]): Unit = {
    println(s"${metricName} [${timeStamp}] metrics: ")
    val json = JsonUtil.toJson(metrics)
    println(json)
//    metrics.foreach { metric =>
//      val (key, value) = metric
//      println(s"${key}: ${value}")
//    }
  }


}
