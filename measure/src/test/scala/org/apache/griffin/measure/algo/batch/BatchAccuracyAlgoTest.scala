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

import org.apache.griffin.measure.algo.batch.BatchAccuracyAlgo
import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.config.reader._
import org.apache.griffin.measure.config.validator._
import org.apache.griffin.measure.connector.direct.DirectDataConnector
import org.apache.griffin.measure.connector.{DataConnector, DataConnectorFactory}
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.rule.expr._
import org.apache.griffin.measure.rule.{ExprValueUtil, RuleAnalyzer, RuleFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Failure, Success, Try}


@RunWith(classOf[JUnitRunner])
class BatchAccuracyAlgoTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {

  val envFile = "src/test/resources/env.json"
  val confFile = "src/test/resources/config.json"
//  val confFile = "{\"name\":\"accu1\",\"type\":\"accuracy\",\"source\":{\"type\":\"avro\",\"version\":\"1.7\",\"config\":{\"file.name\":\"src/test/resources/users_info_src.avro\"}},\"target\":{\"type\":\"avro\",\"version\":\"1.7\",\"config\":{\"file.name\":\"src/test/resources/users_info_target.avro\"}},\"evaluateRule\":{\"sampleRatio\":1,\"rules\":\"$source.user_id + 5 = $target.user_id + (2 + 3) AND $source.first_name + 12 = $target.first_name + (10 + 2) AND $source.last_name = $target.last_name AND $source.address = $target.address AND $source.email = $target.email AND $source.phone = $target.phone AND $source.post_code = $target.post_code AND (15 OR true) WHEN true AND $source.user_id > 10020\"}}"
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
//    sqlContext = new SQLContext(sc)
    sqlContext = new HiveContext(sc)
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
      val sourceDataConnector: DirectDataConnector =
        DataConnectorFactory.getDirectDataConnector(sqlContext, null, userParam.sourceParam,
          ruleAnalyzer.sourceRuleExprs, finalConstMap
        ) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("source data not available!")
          }
          case Failure(ex) => throw ex
        }
      val targetDataConnector: DirectDataConnector =
        DataConnectorFactory.getDirectDataConnector(sqlContext, null, userParam.targetParam,
          ruleAnalyzer.targetRuleExprs, finalConstMap
        ) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("target data not available!")
          }
          case Failure(ex) => throw ex
        }

      // get metadata
//      val sourceMetaData: Iterable[(String, String)] = sourceDataConnector.metaData() match {
//        case Success(md) => md
//        case Failure(ex) => throw ex
//      }
//      val targetMetaData: Iterable[(String, String)] = targetDataConnector.metaData() match {
//        case Success(md) => md
//        case Failure(ex) => throw ex
//      }

      // get data
      val sourceData: RDD[(Product, (Map[String, Any], Map[String, Any]))] = sourceDataConnector.data() match {
        case Success(dt) => dt
        case Failure(ex) => throw ex
      }
      val targetData: RDD[(Product, (Map[String, Any], Map[String, Any]))] = targetDataConnector.data() match {
        case Success(dt) => dt
        case Failure(ex) => throw ex
      }

      // my algo
      val algo = BatchAccuracyAlgo(allParam)

      // accuracy algorithm
      val (accuResult, missingRdd, matchedRdd) = algo.accuracy(sourceData, targetData, ruleAnalyzer)

      println(s"match percentage: ${accuResult.matchPercentage}, total count: ${accuResult.total}")

      missingRdd.map(rec => algo.record2String(rec, ruleAnalyzer.sourceRuleExprs.persistExprs, ruleAnalyzer.targetRuleExprs.persistExprs)).foreach(println)

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

  test ("spark sql") {
    Try {
      val envParam = allParam.envParam
      val userParam = allParam.userParam

      // start time
      val startTime = new Date().getTime()

      // get spark application id
      val applicationId = sc.applicationId

//      val sourceFilePath = "src/test/resources/users_info_src.avro"
//      val targetFilePath = "src/test/resources/users_info_target.avro"
//
//      val sourceDF = sqlContext.read.format("com.databricks.spark.avro").load(sourceFilePath)
//      val targetDF = sqlContext.read.format("com.databricks.spark.avro").load(targetFilePath)

      val sourceTableName = "source"
      val targetTableName = "target"

      val sourceDF = sqlContext.sql("SELECT * FROM default.data_rdm")
      val targetDF = sqlContext.sql("SELECT * FROM default.data_rdm")

//      sourceDF.show(100)
//      targetDF.show(100)

      sourceDF.registerTempTable(sourceTableName)
      targetDF.registerTempTable(targetTableName)

      val sql =
        s"""
          |SELECT COUNT(*) FROM ${sourceTableName} LEFT JOIN ${targetTableName}
          |ON ${sourceTableName}.uid = ${targetTableName}.uid
        """.stripMargin

//      val sql =
//        """
//          |SELECT COUNT(*) FROM source LEFT JOIN target
//          |ON coalesce(source.user_id, 'null') = coalesce(target.user_id, 'null')
//          |AND coalesce(source.first_name, 'null') = coalesce(target.first_name, 'null')
//          |AND coalesce(source.last_name, 'null') = coalesce(target.last_name, 'null')
//          |AND coalesce(source.address, 'null') = coalesce(target.address, 'null')
//          |AND coalesce(source.email, 'null') = coalesce(target.email, 'null')
//          |AND coalesce(source.phone, 'null') = coalesce(target.phone, 'null')
//          |AND coalesce(source.post_code, 'null') = coalesce(target.post_code, 'null')
//          |WHERE (target.user_id IS NULL
//          |AND target.first_name IS NULL
//          |AND target.last_name IS NULL
//          |AND target.address IS NULL
//          |AND target.email IS NULL
//          |AND target.phone IS NULL
//          |AND target.post_code IS NULL)
//        """.stripMargin

//      val sql =
//        """
//          |SELECT * FROM source LEFT JOIN target
//          |ON source.user_id = target.user_id
//          |AND source.first_name = target.first_name
//          |AND source.last_name = target.last_name
//          |AND source.address = target.address
//          |AND source.email = target.email
//          |AND source.phone = target.phone
//          |AND coalesce(source.post_code, 'null') = coalesce(target.post_code, 'null')
//        """.stripMargin

//      val sql =
//        """
//          |SELECT * FROM source WHERE source.post_code IS NULL
//        """.stripMargin

      val result = sqlContext.sql(sql)

      result.show(100)

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

}
