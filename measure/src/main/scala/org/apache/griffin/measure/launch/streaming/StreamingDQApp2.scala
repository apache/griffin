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
package org.apache.griffin.measure.launch.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.params._
import org.apache.griffin.measure.context.streaming.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.context.streaming.metric.CacheResults
import org.apache.griffin.measure.context.{ContextId, DQContext}
import org.apache.griffin.measure.job.builder.DQJobBuilder

case class StreamingDQApp2(globalContext: DQContext,
                           evaluateRuleParam: EvaluateRuleParam
                          ) extends Runnable with Loggable {

  val lock = InfoCacheInstance.genLock("process")
  val appPersist = globalContext.getPersist()

  def run(): Unit = {
    val updateTimeDate = new Date()
    val updateTime = updateTimeDate.getTime
    println(s"===== [${updateTimeDate}] process begins =====")
    val locked = lock.lock(5, TimeUnit.SECONDS)
    if (locked) {
      try {

        TimeInfoCache.startTimeInfoCache

        val startTime = new Date().getTime
        appPersist.log(startTime, s"starting process ...")
        val contextId = ContextId(startTime)

        // create dq context
        val dqContext: DQContext = globalContext.cloneDQContext(contextId)

        // build job
        val dqJob = DQJobBuilder.buildDQJob(dqContext, evaluateRuleParam)

        // dq job execute
        dqJob.execute(dqContext)

        // finish calculation
        finishCalculation(dqContext)

        // end time
        val endTime = new Date().getTime
        appPersist.log(endTime, s"process using time: ${endTime - startTime} ms")

        TimeInfoCache.endTimeInfoCache

        // clean old data
        cleanData(dqContext)

      } catch {
        case e: Throwable => error(s"process error: ${e.getMessage}")
      } finally {
        lock.unlock()
      }
    } else {
      println(s"===== [${updateTimeDate}] process ignores =====")
    }
    val endTime = new Date().getTime
    println(s"===== [${updateTimeDate}] process ends, using ${endTime - updateTime} ms =====")
  }

  // finish calculation for this round
  private def finishCalculation(context: DQContext): Unit = {
    context.dataSources.foreach(_.processFinish)
  }

  // clean old data and old result cache
  private def cleanData(context: DQContext): Unit = {
    try {
      context.dataSources.foreach(_.cleanOldData)

      context.clean()

      val cleanTime = TimeInfoCache.getCleanTime
      CacheResults.refresh(cleanTime)
    } catch {
      case e: Throwable => error(s"clean data error: ${e.getMessage}")
    }
  }

}
