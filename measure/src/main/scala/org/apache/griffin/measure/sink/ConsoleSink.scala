/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.sink

import org.apache.spark.rdd.RDD

import org.apache.griffin.measure.utils.JsonUtil
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * sink metric and record to console, for debug
 */
case class ConsoleSink(config: Map[String, Any], metricName: String, timeStamp: Long)
    extends Sink {

  val block: Boolean = true

  val MaxLogLines = "max.log.lines"

  val maxLogLines: Int = config.getInt(MaxLogLines, 100)

  def available(): Boolean = true

  def start(msg: String): Unit = {
    println(s"[$timeStamp] $metricName start: $msg")
  }
  def finish(): Unit = {
    println(s"[$timeStamp] $metricName finish")
  }

  def log(rt: Long, msg: String): Unit = {
    println(s"[$timeStamp] $rt: $msg")
  }

  def sinkRecords(records: RDD[String], name: String): Unit = {
//    println(s"${metricName} [${timeStamp}] records: ")
//    try {
//      val recordCount = records.count
//      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
//      val maxCount = count.toInt
//      if (maxCount > 0) {
//        val recordsArray = records.take(maxCount)
//        recordsArray.foreach(println)
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
  }

  def sinkRecords(records: Iterable[String], name: String): Unit = {
//    println(s"${metricName} [${timeStamp}] records: ")
//    try {
//      val recordCount = records.size
//      val count = if (maxLogLines < 0) recordCount else scala.math.min(maxLogLines, recordCount)
//      if (count > 0) {
//        records.foreach(println)
//      }
//    } catch {
//      case e: Throwable => error(e.getMessage)
//    }
  }

  def sinkMetrics(metrics: Map[String, Any]): Unit = {
    println(s"$metricName [$timeStamp] metrics: ")
    val json = JsonUtil.toJson(metrics)
    println(json)
  }

}
