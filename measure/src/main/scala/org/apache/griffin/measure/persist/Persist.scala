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

import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.result._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Try


trait Persist extends Loggable with Serializable {
  val timeStamp: Long

  val config: Map[String, Any]

  def available(): Boolean

  def start(msg: String): Unit
  def finish(): Unit

  def log(rt: Long, msg: String): Unit

//  def result(rt: Long, result: Result): Unit
//
//  def records(recs: RDD[String], tp: String): Unit
//  def records(recs: Iterable[String], tp: String): Unit

//  def persistRecords(df: DataFrame, name: String): Unit
  def persistRecords(records: Iterable[String], name: String): Unit
//  def persistMetrics(metrics: Seq[String], name: String): Unit
  def persistMetrics(metrics: Map[String, Any]): Unit

}

//object PersistDataType {
//  final val MISS = "miss"
//  final val MATCH = "match"
//}