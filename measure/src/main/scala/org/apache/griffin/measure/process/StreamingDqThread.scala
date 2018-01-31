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
package org.apache.griffin.measure.process

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.cache.result.CacheResultProcesser
import org.apache.griffin.measure.config.params.user.EvaluateRuleParam
import org.apache.griffin.measure.data.source.DataSource
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.engine.DqEngines
import org.apache.griffin.measure.process.temp.{DataFrameCaches, TableRegisters}
import org.apache.griffin.measure.rule.adaptor.{ProcessDetailsKeys, RuleAdaptorGroup, RunPhase}
import org.apache.griffin.measure.rule.plan._
import org.apache.spark.sql.SQLContext

case class StreamingDqThread(sqlContext: SQLContext,
                             dqEngines: DqEngines,
                             dataSources: Seq[DataSource],
                             evaluateRuleParam: EvaluateRuleParam,
                             persistFactory: PersistFactory,
                             appPersist: Persist
                            ) extends Runnable with Loggable {

  val lock = InfoCacheInstance.genLock("process")

  def run(): Unit = {
    val updateTimeDate = new Date()
    val updateTime = updateTimeDate.getTime
    println(s"===== [${updateTimeDate}] process begins =====")
    val locked = lock.lock(5, TimeUnit.SECONDS)
    if (locked) {
      try {

        val st = new Date().getTime
        appPersist.log(st, s"starting process ...")
        val calcTimeInfo = CalcTimeInfo(st)

        TimeInfoCache.startTimeInfoCache

        // init data sources
        val dsTimeRanges = dqEngines.loadData(dataSources, calcTimeInfo)

        println(s"data source timeRanges: ${dsTimeRanges}")

        // generate rule steps
        val rulePlan = RuleAdaptorGroup.genRulePlan(
          calcTimeInfo, evaluateRuleParam, StreamingProcessType, dsTimeRanges)

        // run rules
        dqEngines.runRuleSteps(calcTimeInfo, rulePlan.ruleSteps)

        val ct = new Date().getTime
        val calculationTimeStr = s"calculation using time: ${ct - st} ms"
        appPersist.log(ct, calculationTimeStr)

        // persist results
        dqEngines.persistAllMetrics(rulePlan.metricExports, persistFactory)

        val rt = new Date().getTime
        val persistResultTimeStr = s"persist result using time: ${rt - ct} ms"
        appPersist.log(rt, persistResultTimeStr)

        // persist records
        dqEngines.persistAllRecords(rulePlan.recordExports, persistFactory, dataSources)

        // update data sources
        dqEngines.updateDataSources(rulePlan.dsUpdates, dataSources)

        val et = new Date().getTime
        val persistTimeStr = s"persist records using time: ${et - rt} ms"
        appPersist.log(et, persistTimeStr)

        TimeInfoCache.endTimeInfoCache

        // clean old data
        cleanData(calcTimeInfo)

//        sqlContext.tables().show(20)

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

  // clean old data and old result cache
  private def cleanData(timeInfo: TimeInfo): Unit = {
    try {
      dataSources.foreach(_.cleanOldData)

      TableRegisters.unregisterRunTempTables(sqlContext, timeInfo.key)
      TableRegisters.unregisterCompileTempTables(timeInfo.key)

      DataFrameCaches.uncacheDataFrames(timeInfo.key)
      DataFrameCaches.clearTrashDataFrames(timeInfo.key)
      DataFrameCaches.clearGlobalTrashDataFrames()

      val cleanTime = TimeInfoCache.getCleanTime
      CacheResultProcesser.refresh(cleanTime)
    } catch {
      case e: Throwable => error(s"clean data error: ${e.getMessage}")
    }
  }

}
