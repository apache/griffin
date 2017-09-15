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
import org.apache.griffin.measure.utils.HdfsUtil
import org.apache.spark.rdd.RDD

// persist result and data to hdfs
case class LoggerPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  val MaxLogLines = "max.log.lines"

  val maxLogLines = try { config.getOrElse(MaxLogLines, 100).toString.toInt } catch { case _ => 100 }

  def available(): Boolean = true

  def start(msg: String): Unit = {
    println(s"[${timeStamp}] ${metricName} start")
  }
  def finish(): Unit = {
    println(s"[${timeStamp}] ${metricName} finish")
  }

  def result(rt: Long, result: Result): Unit = {
    try {
      val resStr = result match {
        case ar: AccuracyResult => {
          s"match percentage: ${ar.matchPercentage}\ntotal count: ${ar.getTotal}\nmiss count: ${ar.getMiss}, match count: ${ar.getMatch}"
        }
        case pr: ProfileResult => {
          s"match percentage: ${pr.matchPercentage}\ntotal count: ${pr.getTotal}\nmiss count: ${pr.getMiss}, match count: ${pr.getMatch}"
        }
        case _ => {
          s"result: ${result}"
        }
      }
      println(s"[${timeStamp}] ${metricName} result: \n${resStr}")
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  // need to avoid string too long
  private def rddRecords(records: RDD[String]): Unit = {
    try {
      val recordCount = records.count.toInt
      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
      if (count > 0) {
        val recordsArray = records.take(count)
//        recordsArray.foreach(println)
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  private def iterableRecords(records: Iterable[String]): Unit = {
    try {
      val recordCount = records.size
      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
      if (count > 0) {
        val recordsArray = records.take(count)
//        recordsArray.foreach(println)
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

  def records(recs: RDD[String], tp: String): Unit = {
    tp match {
      case PersistType.MISS => rddRecords(recs)
      case PersistType.MATCH => rddRecords(recs)
      case _ => {}
    }
  }

  def records(recs: Iterable[String], tp: String): Unit = {
    tp match {
      case PersistType.MISS => iterableRecords(recs)
      case PersistType.MATCH => iterableRecords(recs)
      case _ => {}
    }
  }

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

}
