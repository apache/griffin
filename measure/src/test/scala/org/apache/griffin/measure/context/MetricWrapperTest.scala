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
package org.apache.griffin.measure.context

import org.scalatest._

class MetricWrapperTest extends FlatSpec with Matchers {

  "metric wrapper" should "flush empty if no metric inserted" in {
    val metricWrapper = MetricWrapper("name", "appId")
    metricWrapper.flush should be (Map[Long, Map[String, Any]]())
  }

  it should "flush all metrics inserted" in {
    val metricWrapper = MetricWrapper("test", "appId")
    metricWrapper.insertMetric(1, Map("total" -> 10, "miss"-> 2))
    metricWrapper.insertMetric(1, Map("match" -> 8))
    metricWrapper.insertMetric(2, Map("total" -> 20))
    metricWrapper.insertMetric(2, Map("miss" -> 4))
    metricWrapper.flush should be (Map(
      1L -> Map("name" -> "test", "tmst" -> 1, "value" -> Map("total" -> 10, "miss"-> 2, "match" -> 8),
        "metadata" -> Map("applicationId" -> "appId")),
      2L -> Map("name" -> "test", "tmst" -> 2, "value" -> Map("total" -> 20, "miss"-> 4),
        "metadata" -> Map("applicationId" -> "appId"))
    ))
  }

}
