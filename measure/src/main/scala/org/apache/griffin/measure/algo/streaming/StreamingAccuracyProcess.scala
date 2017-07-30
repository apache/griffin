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
package org.apache.griffin.measure.algo.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.algo.core.AccuracyCore
import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.cache.result.CacheResultProcesser
import org.apache.griffin.measure.connector.direct.DirectDataConnector
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist._
import org.apache.griffin.measure.result.{AccuracyResult, MismatchInfo, TimeStampInfo}
import org.apache.griffin.measure.rule._
import org.apache.griffin.measure.rule.expr._
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success}

case class StreamingAccuracyProcess(sourceDataConnector: DirectDataConnector,
                                    targetDataConnector: DirectDataConnector,
                                    ruleAnalyzer: RuleAnalyzer,
                                    cacheResultProcesser: CacheResultProcesser,
                                    persistFactory: PersistFactory,
                                    appPersist: Persist
                                   ) extends Runnable with Loggable {

  val lock = InfoCacheInstance.genLock("process")

  def run(): Unit = {
//    println(s"cache count: ${cacheResultProcesser.cacheGroup.size}")
    val updateTimeDate = new Date()
    val updateTime = updateTimeDate.getTime
    println(s"===== [${updateTimeDate}] process begins =====")
    val locked = lock.lock(5, TimeUnit.SECONDS)
    if (locked) {
      try {
        val st = new Date().getTime

        TimeInfoCache.startTimeInfoCache

        // get data
        val sourceData = sourceDataConnector.data match {
          case Success(dt) => dt
          case Failure(ex) => throw ex
        }
        val targetData = targetDataConnector.data match {
          case Success(dt) => dt
          case Failure(ex) => throw ex
        }

        sourceData.cache
        targetData.cache

        println(s"sourceData.count: ${sourceData.count}")
        println(s"targetData.count: ${targetData.count}")

        // accuracy algorithm
        val (accuResult, missingRdd, matchedRdd) = accuracy(sourceData, targetData, ruleAnalyzer)
//        println(s"accuResult: ${accuResult}")

        val ct = new Date().getTime
        appPersist.log(ct, s"calculation using time: ${ct - st} ms")

        sourceData.unpersist()
        targetData.unpersist()

        // result of every group
        val matchedGroups = reorgByTimeGroup(matchedRdd)
//        val matchedGroupCount = matchedGroups.count
//        println(s"===== matchedGroupCount: ${matchedGroupCount} =====")

        // get missing results
        val missingGroups = reorgByTimeGroup(missingRdd)
//        val missingGroupCount = missingGroups.count
//        println(s"===== missingGroupCount: ${missingGroupCount} =====")

        val groups = matchedGroups.cogroup(missingGroups)
//        val groupCount = groups.count
//        println(s"===== groupCount: ${groupCount} =====")

        val updateResults = groups.flatMap { group =>
          val (t, (matchData, missData)) = group

          val matchSize = matchData.size
          val missSize = missData.size
          val res = AccuracyResult(missSize, matchSize + missSize)

          val updatedCacheResultOpt = cacheResultProcesser.genUpdateCacheResult(t, updateTime, res)

          updatedCacheResultOpt.flatMap { updatedCacheResult =>
            Some((updatedCacheResult, (t, missData)))
          }
        }

        updateResults.cache

        val updateResultsPart =  updateResults.map(_._1)
        val updateDataPart =  updateResults.map(_._2)

        val updateResultsArray = updateResultsPart.collect()

        // update results cache (in driver)
        // collect action is traversable once action, it will make rdd updateResults empty
        updateResultsArray.foreach { updateResult =>
//          println(s"update result: ${updateResult}")
          cacheResultProcesser.update(updateResult)
          // persist result
          val persist: Persist = persistFactory.getPersists(updateResult.timeGroup)
          persist.result(updateTime, updateResult.result)
        }

        // record missing data and dump old data (in executor)
        updateDataPart.foreach { grp =>
          val (t, datas) = grp
          val persist: Persist = persistFactory.getPersists(t)
          // persist missing data
          val missStrings = datas.map { row =>
            record2String(row, ruleAnalyzer.sourceRuleExprs.persistExprs, ruleAnalyzer.targetRuleExprs.persistExprs)
          }
          persist.records(missStrings, PersistType.MISS)

          // data connector update old data
          val dumpDatas = datas.map { r =>
            val (_, (v, i)) = r
            v ++ i
          }
          sourceDataConnector.updateOldData(t, dumpDatas)
          //          targetDataConnector.updateOldData(t, dumpDatas)    // not correct
        }

        updateResults.unpersist()

        // dump missing rdd   (this part not need for future version, only for current df cache data version)
        val dumpRdd: RDD[Map[String, Any]] = missingRdd.map { r =>
          val (_, (v, i)) = r
          v ++ i
        }
        sourceDataConnector.updateAllOldData(dumpRdd)
        targetDataConnector.updateAllOldData(dumpRdd)    // not correct

        TimeInfoCache.endTimeInfoCache

        // clean old data
        cleanData()

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
  def cleanData(): Unit = {
    try {
      sourceDataConnector.cleanOldData
      targetDataConnector.cleanOldData

      val cleanTime = TimeInfoCache.getCleanTime
      cacheResultProcesser.refresh(cleanTime)
    } catch {
      case e: Throwable => error(s"clean data error: ${e.getMessage}")
    }
  }

  // calculate accuracy between source data and target data
  private def accuracy(sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))],
               targetData: RDD[(Product, (Map[String, Any], Map[String, Any]))],
               ruleAnalyzer: RuleAnalyzer) = {
    // 1. cogroup
    val allKvs = sourceData.cogroup(targetData)

    // 2. accuracy calculation
    val (accuResult, missingRdd, matchedRdd) = AccuracyCore.accuracy(allKvs, ruleAnalyzer)

    (accuResult, missingRdd, matchedRdd)
  }

  private def reorgByTimeGroup(rdd: RDD[(Product, (Map[String, Any], Map[String, Any]))]
                      ): RDD[(Long, (Product, (Map[String, Any], Map[String, Any])))] = {
    rdd.flatMap { row =>
      val (key, (value, info)) = row
      val b: Option[(Long, (Product, (Map[String, Any], Map[String, Any])))] = info.get(TimeStampInfo.key) match {
        case Some(t: Long) => Some((t, row))
        case _ => None
      }
      b
    }
  }

  // convert data into a string
  def record2String(rec: (Product, (Map[String, Any], Map[String, Any])), dataPersist: Iterable[Expr], infoPersist: Iterable[Expr]): String = {
    val (key, (data, info)) = rec
    val persistData = getPersistMap(data, dataPersist)
    val persistInfo = info.mapValues { value =>
      value match {
        case vd: Map[String, Any] => getPersistMap(vd, infoPersist)
        case v => v
      }
    }.map(identity)
    s"${persistData} [${persistInfo}]"
  }

  // get the expr value map of the persist expressions
  private def getPersistMap(data: Map[String, Any], persist: Iterable[Expr]): Map[String, Any] = {
    val persistMap = persist.map(e => (e._id, e.desc)).toMap
    data.flatMap { pair =>
      val (k, v) = pair
      persistMap.get(k) match {
        case Some(d) => Some((d -> v))
        case _ => None
      }
    }
  }

}
