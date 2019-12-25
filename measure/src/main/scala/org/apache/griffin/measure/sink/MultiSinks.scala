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

/**
 * sink metric and record in multiple ways
 */
case class MultiSinks(sinkIter: Iterable[Sink]) extends Sink {

  val block: Boolean = false

  val headSinkOpt: Option[Sink] = sinkIter.headOption

  val metricName: String = headSinkOpt.map(_.metricName).getOrElse("")

  val timeStamp: Long = headSinkOpt.map(_.timeStamp).getOrElse(0)

  val config: Map[String, Any] = Map[String, Any]()

  def available(): Boolean = {
    sinkIter.exists(_.available())
  }

  def start(msg: String): Unit = {
    sinkIter.foreach(_.start(msg))
  }

  def finish(): Unit = {
    sinkIter.foreach(_.finish())
  }

  def log(rt: Long, msg: String): Unit = {
    sinkIter.foreach { sink =>
      try {
        sink.log(rt, msg)
      } catch {
        case e: Throwable => error(s"log error: ${e.getMessage}", e)
      }
    }
  }

  def sinkRecords(records: RDD[String], name: String): Unit = {
    sinkIter.foreach { sink =>
      try {
        sink.sinkRecords(records, name)
      } catch {
        case e: Throwable => error(s"sink records error: ${e.getMessage}", e)
      }
    }
  }

  def sinkRecords(records: Iterable[String], name: String): Unit = {
    sinkIter.foreach { sink =>
      try {
        sink.sinkRecords(records, name)
      } catch {
        case e: Throwable => error(s"sink records error: ${e.getMessage}", e)
      }
    }
  }

  def sinkMetrics(metrics: Map[String, Any]): Unit = {
    sinkIter.foreach { sink =>
      try {
        sink.sinkMetrics(metrics)
      } catch {
        case e: Throwable => error(s"sink metrics error: ${e.getMessage}", e)
      }
    }
  }

}
