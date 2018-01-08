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
import org.apache.griffin.measure.rule.adaptor.{RuleAdaptorGroup, RunPhase}
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
        val dsTmsts = dqEngines.loadData(dataSources, calcTimeInfo)

        println(s"data sources timestamps: ${dsTmsts}")

        // generate rule steps
//        val ruleSteps = RuleAdaptorGroup.genRuleSteps(
//          CalcTimeInfo(st), evaluateRuleParam, dsTmsts)
        val rulePlan = RuleAdaptorGroup.genRulePlan(
          calcTimeInfo, evaluateRuleParam, StreamingProcessType)

//        ruleSteps.foreach(println)

        // run rules
//        dqEngines.runRuleSteps(ruleSteps)
        dqEngines.runRuleSteps(calcTimeInfo, rulePlan.ruleSteps)

        val ct = new Date().getTime
        val calculationTimeStr = s"calculation using time: ${ct - st} ms"
//        println(calculationTimeStr)
        appPersist.log(ct, calculationTimeStr)

        // persist results
//        val timeGroups = dqEngines.persistAllMetrics(ruleSteps, persistFactory)
        dqEngines.persistAllMetrics(calcTimeInfo, rulePlan.metricExports,
          StreamingProcessType, persistFactory)
//        println(s"--- timeGroups: ${timeGroups}")

        val rt = new Date().getTime
        val persistResultTimeStr = s"persist result using time: ${rt - ct} ms"
        appPersist.log(rt, persistResultTimeStr)

        // persist records
        dqEngines.persistAllRecords(calcTimeInfo, rulePlan.recordExports,
          StreamingProcessType, persistFactory, dataSources)

        val et = new Date().getTime
        val persistTimeStr = s"persist records using time: ${et - rt} ms"
        appPersist.log(et, persistTimeStr)

//        val dfs = dqEngines.collectUpdateRDDs(ruleSteps, timeGroups.toSet)
//        dfs.foreach(_._2.cache())
//        dfs.foreach { pr =>
//          val (step, df) = pr
//          val cnt = df.count
//          println(s"step [${step.name}] group count: ${cnt}")
//        }
//
//        val lt = new Date().getTime
//        val collectRddTimeStr = s"collect records using time: ${lt - rt} ms"
////        println(collectRddTimeStr)
//        appPersist.log(lt, collectRddTimeStr)
//
//        // persist records
//        dqEngines.persistAllRecords(dfs, persistFactory)
////        dqEngines.persistAllRecords(ruleSteps, persistFactory, timeGroups)
//
//        // update data source
//        dqEngines.updateDataSources(dfs, dataSources)
////        dqEngines.updateDataSources(ruleSteps, dataSources, timeGroups)
//
//        dfs.foreach(_._2.unpersist())

        TimeInfoCache.endTimeInfoCache

//        sqlContext.tables().show(20)

        // cache global data
//        val globalTables = TableRegisters.getRunGlobalTables
//        globalTables.foreach { gt =>
//          val df = sqlContext.table(gt)
//          df.cache
//        }

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

//  // calculate accuracy between source data and target data
//  private def accuracy(sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))],
//               targetData: RDD[(Product, (Map[String, Any], Map[String, Any]))],
//               ruleAnalyzer: RuleAnalyzer) = {
//    // 1. cogroup
//    val allKvs = sourceData.cogroup(targetData)
//
//    // 2. accuracy calculation
//    val (accuResult, missingRdd, matchedRdd) = AccuracyCore.accuracy(allKvs, ruleAnalyzer)
//
//    (accuResult, missingRdd, matchedRdd)
//  }
//
//  private def reorgByTimeGroup(rdd: RDD[(Product, (Map[String, Any], Map[String, Any]))]
//                      ): RDD[(Long, (Product, (Map[String, Any], Map[String, Any])))] = {
//    rdd.flatMap { row =>
//      val (key, (value, info)) = row
//      val b: Option[(Long, (Product, (Map[String, Any], Map[String, Any])))] = info.get(TimeStampInfo.key) match {
//        case Some(t: Long) => Some((t, row))
//        case _ => None
//      }
//      b
//    }
//  }
//
//  // convert data into a string
//  def record2String(rec: (Product, (Map[String, Any], Map[String, Any])), dataPersist: Iterable[Expr], infoPersist: Iterable[Expr]): String = {
//    val (key, (data, info)) = rec
//    val persistData = getPersistMap(data, dataPersist)
//    val persistInfo = info.mapValues { value =>
//      value match {
//        case vd: Map[String, Any] => getPersistMap(vd, infoPersist)
//        case v => v
//      }
//    }.map(identity)
//    s"${persistData} [${persistInfo}]"
//  }
//
//  // get the expr value map of the persist expressions
//  private def getPersistMap(data: Map[String, Any], persist: Iterable[Expr]): Map[String, Any] = {
//    val persistMap = persist.map(e => (e._id, e.desc)).toMap
//    data.flatMap { pair =>
//      val (k, v) = pair
//      persistMap.get(k) match {
//        case Some(d) => Some((d -> v))
//        case _ => None
//      }
//    }
//  }

}
