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
package org.apache.griffin.measure.context.writer

import org.apache.spark.rdd.RDD

/**
  * persist metric and record in multiple ways
  */
case class MultiPersists(persists: Iterable[Persist]) extends Persist {

  val block: Boolean = false

  val metricName: String = persists match {
    case Nil => ""
    case _ => persists.head.metricName
  }

  val timeStamp: Long = persists match {
    case Nil => 0
    case _ => persists.head.timeStamp
  }

  val config: Map[String, Any] = Map[String, Any]()

  def available(): Boolean = { persists.exists(_.available()) }

  def start(msg: String): Unit = { persists.foreach(_.start(msg)) }
  def finish(): Unit = { persists.foreach(_.finish()) }

  def log(rt: Long, msg: String): Unit = {
    persists.foreach { persist =>
      try {
        persist.log(rt, msg)
      } catch {
        case e: Throwable => error(s"log error: ${e.getMessage}")
      }
    }
  }

  def persistRecords(records: RDD[String], name: String): Unit = {
    persists.foreach { persist =>
      try {
        persist.persistRecords(records, name)
      } catch {
        case e: Throwable => error(s"persist records error: ${e.getMessage}")
      }
    }
  }
  def persistRecords(records: Iterable[String], name: String): Unit = {
    persists.foreach { persist =>
      try {
        persist.persistRecords(records, name)
      } catch {
        case e: Throwable => error(s"persist records error: ${e.getMessage}")
      }
    }
  }
  def persistMetrics(metrics: Map[String, Any]): Unit = {
    persists.foreach { persist =>
      try {
        persist.persistMetrics(metrics)
      } catch {
        case e: Throwable => error(s"persist metrics error: ${e.getMessage}")
      }
    }
  }

}
