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

package org.apache.griffin.measure.context

import scala.collection.mutable.{Map => MutableMap}

/**
 * wrap metrics into one, each calculation produces one metric map
 */
case class MetricWrapper(name: String, applicationId: String) extends Serializable {

  val _Name = "name"
  val _Timestamp = "tmst"
  val _Value = "value"
  val _Metadata = "metadata"

  val metrics: MutableMap[Long, Map[String, Any]] = MutableMap()

  def insertMetric(timestamp: Long, value: Map[String, Any]): Unit = {
    val newValue = metrics.get(timestamp) match {
      case Some(v) => v ++ value
      case _ => value
    }
    metrics += (timestamp -> newValue)
  }

  def flush: Map[Long, Map[String, Any]] = {
    metrics.toMap.map { pair =>
      val (timestamp, value) = pair
      (
        timestamp,
        Map[String, Any](
          _Name -> name,
          _Timestamp -> timestamp,
          _Value -> value,
          _Metadata -> Map("applicationId" -> applicationId)))
    }
  }

}
