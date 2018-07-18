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

import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.griffin.measure.utils.{HttpUtil, JsonUtil, TimeUtil}
import org.apache.spark.rdd.RDD

import scala.concurrent.Future

/**
  * persist metric and record through http request
  */
case class HttpSink(config: Map[String, Any], metricName: String,
                    timeStamp: Long, block: Boolean
                      ) extends Sink {

  val Api = "api"
  val Method = "method"
  val OverTime = "over.time"
  val Retry = "retry"

  val api = config.getString(Api, "")
  val method = config.getString(Method, "post")
  val overTime = TimeUtil.milliseconds(config.getString(OverTime, "")).getOrElse(-1L)
  val retry = config.getInt(Retry, 10)

  val _Value = "value"

  def available(): Boolean = {
    api.nonEmpty
  }

  def start(msg: String): Unit = {}
  def finish(): Unit = {}

  private def httpResult(dataMap: Map[String, Any]) = {
    try {
      val data = JsonUtil.toJson(dataMap)
      // http request
      val params = Map[String, Object]()
      val header = Map[String, Object](("Content-Type","application/json"))

      def func(): (Long, Future[Boolean]) = {
        import scala.concurrent.ExecutionContext.Implicits.global
        (timeStamp, Future(HttpUtil.httpRequest(api, method, params, header, data)))
      }
      if (block) SinkTaskRunner.addBlockTask(func _, retry, overTime)
      else SinkTaskRunner.addNonBlockTask(func _, retry)
    } catch {
      case e: Throwable => error(e.getMessage)
    }

  }

  def log(rt: Long, msg: String): Unit = {}

  def persistRecords(records: RDD[String], name: String): Unit = {}
  def persistRecords(records: Iterable[String], name: String): Unit = {}

  def persistMetrics(metrics: Map[String, Any]): Unit = {
    httpResult(metrics)
  }

}
