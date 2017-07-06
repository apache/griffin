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
package org.apache.griffin.measure.algo.batch

import java.util.Date

import org.apache.griffin.measure.algo.ProfileAlgo
import org.apache.griffin.measure.algo.core.ProfileCore
import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.connector._
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.result._
import org.apache.griffin.measure.rule.expr.{Expr, StatementExpr}
import org.apache.griffin.measure.rule.{ExprValueUtil, RuleAnalyzer, RuleFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

// profile algorithm for batch mode
case class BatchProfileAlgo(allParam: AllParam) extends ProfileAlgo {
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
      val finalConstMap = finalConstExprValueMap.headOption match {
        case Some(m) => m
        case _ => Map[String, Any]()
      }

      // data connector
      val sourceDataConnector: BatchDataConnector =
      DataConnectorFactory.getBatchDataConnector(sqlContext, userParam.sourceParam,
        ruleAnalyzer.sourceRuleExprs, finalConstMap
      ) match {
        case Success(cntr) => {
          if (cntr.available) cntr
          else throw new Exception("source data connection error!")
        }
        case Failure(ex) => throw ex
      }

      // get metadata
      //      val sourceMetaData: Iterable[(String, String)] = sourceDataConnector.metaData() match {
      //        case Success(md) => md
      //        case _ => throw new Exception("source metadata error!")
      //      }

      // get data
      val sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))] = sourceDataConnector.data() match {
        case Success(dt) => dt
        case Failure(ex) => throw ex
      }

      // profile algorithm
      val (profileResult, missingRdd, matchedRdd) = profile(sourceData, ruleAnalyzer)

      // end time
      val endTime = new Date().getTime
      persist.log(endTime, s"calculation using time: ${endTime - startTime} ms")

      // persist result
      persist.result(endTime, profileResult)
      val matchedRecords = matchedRdd.map(record2String(_, ruleAnalyzer.sourceRuleExprs.persistExprs))
      persist.matchRecords(matchedRecords)

      // persist end time
      val persistEndTime = new Date().getTime
      persist.log(persistEndTime, s"persist using time: ${persistEndTime - endTime} ms")

      // finish
      persist.finish()

      // context stop
      sc.stop
    }
  }

  def wrapInitData(data: Map[String, Any]): (Map[String, Any], Map[String, Any]) = {
    (data, Map[String, Any]())
  }

  // calculate profile from source data
  def profile(sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))], ruleAnalyzer: RuleAnalyzer
              ) = {
    // 1. profile calculation
    val (profileResult, missingRdd, matchedRdd) = ProfileCore.profile(sourceData, ruleAnalyzer)

    (profileResult, missingRdd, matchedRdd)
  }

  // convert data into a string
  def record2String(rec: (Product, (Map[String, Any], Map[String, Any])), sourcePersist: Iterable[Expr]): String = {
    val (key, (data, info)) = rec
    val persistData = getPersistMap(data, sourcePersist)
    val persistInfo = info
    if (persistInfo.size > 0) s"${persistData} [${persistInfo}]" else s"${persistData}"
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
