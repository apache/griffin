/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.algo.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.griffin.measure.algo.AccuracyAlgo
import org.apache.griffin.measure.algo.core.AccuracyCore
import org.apache.griffin.measure.cache.InfoCacheInstance
import org.apache.griffin.measure.config.params.AllParam
import org.apache.griffin.measure.connector._
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.result.AccuracyResult
import org.apache.griffin.measure.rule.{ExprValueUtil, RuleAnalyzer, RuleFactory}
import org.apache.griffin.measure.rule.expr.{Expr, StatementExpr}
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}


case class StreamingAccuracyAlgo(allParam: AllParam) extends AccuracyAlgo {
  val envParam = allParam.envParam
  val userParam = allParam.userParam

  def run(): Try[_] = {
    Try {
      val metricName = userParam.name

      val sparkParam = envParam.sparkParam

      val conf = new SparkConf().setAppName(metricName)
      val sc = new SparkContext(conf)
      sc.setLogLevel(sparkParam.logLevel)
      val sqlContext = new HiveContext(sc)

      val interval = TimeUtil.milliseconds(sparkParam.batchInterval) match {
        case Some(interval) => Milliseconds(interval)
        case _ => throw new Exception("invalid batch interval")
      }
      val ssc = new StreamingContext(sc, interval)
      ssc.checkpoint(sparkParam.cpDir)

      // start time
      val startTime = new Date().getTime()

      // get persists to persist measure result
      val persist: Persist = PersistFactory(envParam.persistParams, metricName).getPersists(startTime)

      // get spark application id
      val applicationId = sc.applicationId

      // persist start id
      persist.start(applicationId)

      // generate rule from rule param, generate rule analyzer
      val ruleFactory = RuleFactory(userParam.evaluateRuleParam)
      val rule: StatementExpr = ruleFactory.generateRule()
      val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

      // const expr value map
      val constExprValueMap = ExprValueUtil.genExprValueMaps(None, ruleAnalyzer.constCacheExprs, Map[String, Any]())
      val finalConstExprValueMap = ExprValueUtil.updateExprValueMaps(ruleAnalyzer.constFinalCacheExprs, constExprValueMap)

      // data connector
      val sourceDataConnector: BatchDataConnector =
      DataConnectorFactory.getDataConnector(sqlContext, ssc, userParam.sourceParam,
        ruleAnalyzer.sourceRuleExprs, finalConstExprValueMap.head
      ) match {
        case Success(cntr) => {
          if (cntr.available) cntr
          else throw new Exception("source data connection error!")
        }
        case Failure(ex) => throw ex
      }
      val targetDataConnector: BatchDataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, ssc, userParam.targetParam,
          ruleAnalyzer.targetRuleExprs, finalConstExprValueMap.head
        ) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("target data connection error!")
          }
          case Failure(ex) => throw ex
        }

      InfoCacheInstance.initInstance(envParam.infoCacheParams, metricName)
      InfoCacheInstance.init

      // process thread
      case class Process() extends Runnable {
        val lock = InfoCacheInstance.genLock
        def run(): Unit = {
          val locked = lock.lock(5, TimeUnit.SECONDS)
          if (locked) {
            try {
              val st = new Date().getTime
              // get data
              val sourceData = sourceDataConnector.data match {
                case Success(dt) => dt
                case Failure(ex) => throw ex
              }
              val targetData = targetDataConnector.data match {
                case Success(dt) => dt
                case Failure(ex) => throw ex
              }

              // accuracy algorithm
              val (accuResult, missingRdd, matchedRdd) = accuracy(sourceData, targetData, ruleAnalyzer)

              val et = new Date().getTime

              // persist time
              persist.log(et, s"calculation using time: ${et - st} ms")

              // persist result
              persist.result(et, accuResult)
              val missingRecords = missingRdd.map(record2String(_, ruleAnalyzer.sourceRuleExprs.persistExprs, ruleAnalyzer.targetRuleExprs.persistExprs))
              persist.missRecords(missingRecords)

              val pet = new Date().getTime
              persist.log(pet, s"persist using time: ${pet - et} ms")
            } finally {
              lock.unlock()
            }
          }
        }
      }

      // fixme: thread pool

      // get data
//      val sourceData: RDD[(Product, Map[String, Any])] = sourceDataConnector.data() match {
//        case Success(dt) => dt
//        case Failure(ex) => throw ex
//      }
//      val targetData: RDD[(Product, Map[String, Any])] = targetDataConnector.data() match {
//        case Success(dt) => dt
//        case Failure(ex) => throw ex
//      }
//
//      // accuracy algorithm
//      val (accuResult, missingRdd, matchedRdd) = accuracy(sourceData, targetData, ruleAnalyzer)
//
//      // end time
//      val endTime = new Date().getTime
//      persist.log(endTime, s"calculation using time: ${endTime - startTime} ms")
//
//      // persist result
//      persist.result(endTime, accuResult)
//      val missingRecords = missingRdd.map(record2String(_, ruleAnalyzer.sourceRuleExprs.persistExprs, ruleAnalyzer.targetRuleExprs.persistExprs))
//      persist.missRecords(missingRecords)
//
//      // persist end time
//      val persistEndTime = new Date().getTime
//      persist.log(persistEndTime, s"persist using time: ${persistEndTime - endTime} ms")
//
//      // finish
//      persist.finish()

      ssc.start()
      ssc.awaitTermination()
      ssc.stop(stopSparkContext=true, stopGracefully=true)

      // context stop
      sc.stop

      InfoCacheInstance.close

      persist.finish()
    }
  }

  // calculate accuracy between source data and target data
  def accuracy(sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))],
               targetData: RDD[(Product, (Map[String, Any], Map[String, Any]))],
               ruleAnalyzer: RuleAnalyzer) = {
    // 1. cogroup
    val allKvs = sourceData.cogroup(targetData)

    // 2. accuracy calculation
    val (accuResult, missingRdd, matchedRdd) = AccuracyCore.accuracy(allKvs, ruleAnalyzer)

    (accuResult, missingRdd, matchedRdd)
  }

  // convert data into a string
  def record2String(rec: (Product, (Map[String, Any], Map[String, Any])), sourcePersist: Iterable[Expr], targetPersist: Iterable[Expr]): String = {
    val (key, (data, info)) = rec
    val persistData = getPersistMap(data, sourcePersist)
    val persistInfo = info.mapValues { value =>
      value match {
        case vd: Map[String, Any] => getPersistMap(vd, targetPersist)
        case v => v
      }
    }
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
