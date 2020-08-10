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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * sink records and metrics in memory for test.
 *
 * @param config sink configurations
 * @param jobName
 * @param timeStamp
 * @param block
 */
case class CustomSink(config: Map[String, Any], jobName: String, timeStamp: Long, block: Boolean)
    extends Sink {
  def validate(): Boolean = true

  def log(rt: Long, msg: String): Unit = {}

  val allRecords: ListBuffer[String] = mutable.ListBuffer[String]()

  override def sinkRecords(records: RDD[String], name: String): Unit = {
    allRecords ++= records.collect()
  }

  override def sinkRecords(records: Iterable[String], name: String): Unit = {
    allRecords ++= records
  }

  val allMetrics: mutable.Map[String, Any] = mutable.Map[String, Any]()

  override def sinkMetrics(metrics: Map[String, Any]): Unit = {
    allMetrics ++= metrics
  }

  override def sinkBatchRecords(dataset: DataFrame, key: Option[String] = None): Unit = {
    allRecords ++= dataset.toJSON.rdd.collect()
  }
}
