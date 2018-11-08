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
package org.apache.griffin.measure.step.write

import org.apache.griffin.measure.context.DQContext

/**
  * flush final metric map in context and write
  */
case class MetricFlushStep() extends WriteStep {

  val name: String = ""
  val inputName: String = ""
  val writeTimestampOpt: Option[Long] = None

  def execute(context: DQContext): Boolean = {
    context.metricWrapper.flush.foldLeft(true) { (ret, pair) =>
      val (t, metric) = pair
      val pr = try {
        context.getSink(t).sinkMetrics(metric)
        true
      } catch {
        case e: Throwable =>
          error(s"flush metrics error: ${e.getMessage}", e)
          false
      }
      ret && pr
    }
  }

}
