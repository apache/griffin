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
import org.apache.spark.sql.DataFrame

import org.apache.griffin.measure.utils.JsonUtil
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * sink metric and record to console, for debug
 */
case class ConsoleSink(config: Map[String, Any], jobName: String, timeStamp: Long) extends Sink {

  val block: Boolean = true

  val Truncate: String = "truncate"
  val truncateRecords: Boolean = config.getBoolean(Truncate, defValue = true)

  val NumberOfRows: String = "numRows"
  val numRows: Int = config.getInt(NumberOfRows, 10)

  def validate(): Boolean = true

  override def open(msg: String): Unit = {
    griffinLogger.info(
      s"Opened ConsoleSink for job with name '$jobName', " +
        s"timestamp '$timeStamp' and applicationId '$msg'")
  }

  override def close(): Unit = {
    griffinLogger.info(
      s"Closed ConsoleSink for job with name '$jobName' and timestamp '$timeStamp'")
  }

  def sinkRecords(records: RDD[String], name: String): Unit = {}

  def sinkRecords(records: Iterable[String], name: String): Unit = {}

  def sinkMetrics(metrics: Map[String, Any]): Unit = {
    griffinLogger.info(s"$jobName [$timeStamp] metrics:\n${JsonUtil.toJson(metrics)}")
  }

  override def sinkBatchRecords(dataset: DataFrame, key: Option[String] = None): Unit = {
    dataset.show(numRows, truncateRecords)
  }

}
