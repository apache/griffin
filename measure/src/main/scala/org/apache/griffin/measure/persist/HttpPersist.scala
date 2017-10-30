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

import org.apache.griffin.measure.result._
import org.apache.griffin.measure.utils.{HttpUtil, JsonUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Try
import org.apache.griffin.measure.utils.ParamUtil._

// persist result by http way
case class HttpPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  val Api = "api"
  val Method = "method"

  val api = config.getString(Api, "")
  val method = config.getString(Method, "post")

  val _Value = "value"

  def available(): Boolean = {
    api.nonEmpty
  }

  def start(msg: String): Unit = {}
  def finish(): Unit = {}

//  def result(rt: Long, result: Result): Unit = {
//    result match {
//      case ar: AccuracyResult => {
//        val dataMap = Map[String, Any](("name" -> metricName), ("tmst" -> timeStamp), ("total" -> ar.getTotal), ("matched" -> ar.getMatch))
//        httpResult(dataMap)
//      }
//      case pr: ProfileResult => {
//        val dataMap = Map[String, Any](("name" -> metricName), ("tmst" -> timeStamp), ("total" -> pr.getTotal), ("matched" -> pr.getMatch))
//        httpResult(dataMap)
//      }
//      case _ => {
//        info(s"result: ${result}")
//      }
//    }
//  }

  private def httpResult(dataMap: Map[String, Any]) = {
    try {
      val data = JsonUtil.toJson(dataMap)
      // post
      val params = Map[String, Object]()
      val header = Map[String, Object]()

      def func(): Boolean = {
        HttpUtil.httpRequest(api, method, params, header, data)
      }

      PersistThreadPool.addTask(func _, 10)

//      val status = HttpUtil.httpRequest(api, method, params, header, data)
//      info(s"${method} to ${api} response status: ${status}")
    } catch {
      case e: Throwable => error(e.getMessage)
    }

  }

//  def records(recs: RDD[String], tp: String): Unit = {}
//  def records(recs: Iterable[String], tp: String): Unit = {}

//  def missRecords(records: RDD[String]): Unit = {}
//  def matchRecords(records: RDD[String]): Unit = {}

  def log(rt: Long, msg: String): Unit = {}

//  def persistRecords(df: DataFrame, name: String): Unit = {}
  def persistRecords(records: Iterable[String], name: String): Unit = {}

//  def persistMetrics(metrics: Seq[String], name: String): Unit = {
//    val maps = metrics.flatMap { m =>
//      try {
//        Some(JsonUtil.toAnyMap(m) ++ Map[String, Any](("name" -> metricName), ("tmst" -> timeStamp)))
//      } catch {
//        case e: Throwable => None
//      }
//    }
//    maps.foreach { map =>
//      httpResult(map)
//    }
//  }

  def persistMetrics(metrics: Map[String, Any]): Unit = {
    val head = Map[String, Any](("name" -> metricName), ("tmst" -> timeStamp))
    val result = head + (_Value -> metrics)
    httpResult(result)
  }

}
