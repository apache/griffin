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

import org.apache.griffin.measure.algo._
import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.config.params.user.EvaluateRuleParam
import org.apache.griffin.measure.data.source.DataSource
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.engine.DqEngines
import org.apache.griffin.measure.rules.adaptor.RuleAdaptorGroup

case class StreamingDqThread(dqEngines: DqEngines,
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

        TimeInfoCache.startTimeInfoCache

        // init data sources
        dqEngines.loadData(dataSources)

        // generate rule steps
        val ruleSteps = RuleAdaptorGroup.genConcreteRuleSteps(evaluateRuleParam, StreamingProcessType)

        // run rules
        dqEngines.runRuleSteps(ruleSteps)

        val ct = new Date().getTime
        appPersist.log(ct, s"calculation using time: ${ct - st} ms")

        // persist results
        dqEngines.persistAllResults(ruleSteps, appPersist)

        TimeInfoCache.endTimeInfoCache

        val et = new Date().getTime
        appPersist.log(et, s"persist using time: ${et - ct} ms")

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
//  def cleanData(): Unit = {
//    try {
//      sourceDataConnector.cleanOldData
//      targetDataConnector.cleanOldData
//
//      val cleanTime = TimeInfoCache.getCleanTime
//      cacheResultProcesser.refresh(cleanTime)
//    } catch {
//      case e: Throwable => error(s"clean data error: ${e.getMessage}")
//    }
//  }
//
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
