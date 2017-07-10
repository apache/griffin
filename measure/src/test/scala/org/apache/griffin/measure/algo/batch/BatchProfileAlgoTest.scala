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
package org.apache.griffin.measure.algo

import java.util.Date

import org.apache.griffin.measure.algo.batch.BatchProfileAlgo
import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.config.reader._
import org.apache.griffin.measure.config.validator._
import org.apache.griffin.measure.connector.{BatchDataConnector, DataConnector, DataConnectorFactory}
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.rule.expr._
import org.apache.griffin.measure.rule.{ExprValueUtil, RuleAnalyzer, RuleFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Failure, Success, Try}


@RunWith(classOf[JUnitRunner])
class BatchProfileAlgoTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {

  val envFile = "src/test/resources/env.json"
  val confFile = "src/test/resources/config-profile.json"
  val envFsType = "local"
  val userFsType = "local"

  val args = Array(envFile, confFile)

  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  var allParam: AllParam = _

  before {
    // read param files
    val envParam = readParamFile[EnvParam](envFile, envFsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val userParam = readParamFile[UserParam](confFile, userFsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    allParam = AllParam(envParam, userParam)

    // validate param files
    validateParams(allParam) match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-3)
      }
      case _ => {
        info("params validation pass")
      }
    }

    val metricName = userParam.name
    val conf = new SparkConf().setMaster("local[*]").setAppName(metricName)
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  test("algorithm") {
    Try {
      val envParam = allParam.envParam
      val userParam = allParam.userParam

      // start time
      val startTime = new Date().getTime()

      // get spark application id
      val applicationId = sc.applicationId

      // rules
      val ruleFactory = RuleFactory(userParam.evaluateRuleParam)
      val rule: StatementExpr = ruleFactory.generateRule()
      val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

      ruleAnalyzer.constCacheExprs.foreach(println)
      ruleAnalyzer.constFinalCacheExprs.foreach(println)

      // global cache data
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
            else throw new Exception("source data not available!")
          }
          case Failure(ex) => throw ex
        }

      // get data
      val sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))] = sourceDataConnector.data() match {
        case Success(dt) => dt
        case Failure(ex) => throw ex
      }

      // my algo
      val algo = BatchProfileAlgo(allParam)

      // profile algorithm
      val (profileResult, missingRdd, matchedRdd) = algo.profile(sourceData, ruleAnalyzer)

      println(s"match percentage: ${profileResult.matchPercentage}, match count: ${profileResult.matchCount}, total count: ${profileResult.totalCount}")

      matchedRdd.map(rec => algo.record2String(rec, ruleAnalyzer.sourceRuleExprs.persistExprs)).foreach(println)

      // end time
      val endTime = new Date().getTime
      println(s"using time: ${endTime - startTime} ms")
    } match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-4)
      }
      case _ => {
        info("calculation finished")
      }
    }
  }

  private def readParamFile[T <: Param](file: String, fsType: String)(implicit m : Manifest[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file, fsType)
    paramReader.readConfig[T]
  }

  private def validateParams(allParam: AllParam): Try[Boolean] = {
    val allParamValidator = AllParamValidator()
    allParamValidator.validate(allParam)
  }

}
