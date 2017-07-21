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

import org.apache.griffin.measure.algo.batch.BatchAccuracyAlgo
import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
import org.apache.griffin.measure.cache.result._
import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.config.reader._
import org.apache.griffin.measure.config.validator._
import org.apache.griffin.measure.connector.direct.DirectDataConnector
import org.apache.griffin.measure.connector.{DataConnector, DataConnectorFactory}
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.{Persist, PersistFactory, PersistType}
import org.apache.griffin.measure.result._
import org.apache.griffin.measure.rule.expr._
import org.apache.griffin.measure.rule.{ExprValueUtil, RuleAnalyzer, RuleFactory}
import org.apache.griffin.measure.utils.{HdfsUtil, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Failure, Success, Try}


@RunWith(classOf[JUnitRunner])
class StreamingAccuracyAlgoTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {

  val envFile = "src/test/resources/env-streaming.json"
  val confFile = "src/test/resources/config-streaming.json"
  val envFsType = "local"
  val userFsType = "local"

  val args = Array(envFile, confFile)

  var sc: SparkContext = _
  var sqlContext: SQLContext = _
//  val ssc: StreamingContext = _

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
    sc.setLogLevel(envParam.sparkParam.logLevel)
//    sqlContext = new SQLContext(sc)
    sqlContext = new HiveContext(sc)

    val a = sqlContext.sql("select * from s1 limit 10")
    //    val a = sqlContext.sql("show tables")
    a.show(10)

    val b = HdfsUtil.existPath("/griffin/streaming")
    println(b)
  }

  test("algorithm") {
    val envParam = allParam.envParam
    val userParam = allParam.userParam
    val metricName = userParam.name
    val sparkParam = envParam.sparkParam
    val cleanerParam = envParam.cleanerParam

    val batchInterval = TimeUtil.milliseconds(sparkParam.batchInterval) match {
      case Some(interval) => Milliseconds(interval)
      case _ => throw new Exception("invalid batch interval")
    }
    val ssc = new StreamingContext(sc, batchInterval)
    ssc.checkpoint(sparkParam.cpDir)

//    def createContext(): StreamingContext = {
//      ;
//    }

    // start time
    val startTime = new Date().getTime()

    val persistFactory = PersistFactory(envParam.persistParams, metricName)

    // get persists to persist measure result
    val appPersist: Persist = persistFactory.getPersists(startTime)

    // get spark application id
    val applicationId = sc.applicationId

    // persist start id
    appPersist.start(applicationId)

    InfoCacheInstance.initInstance(envParam.infoCacheParams, metricName)
    InfoCacheInstance.init

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
    val sourceDataConnector: DirectDataConnector =
      DataConnectorFactory.getDirectDataConnector(sqlContext, ssc, userParam.sourceParam,
        ruleAnalyzer.sourceRuleExprs, finalConstMap
      ) match {
        case Success(cntr) => {
          if (cntr.available) cntr
          else throw new Exception("source data connection error!")
        }
        case Failure(ex) => throw ex
      }
    val targetDataConnector: DirectDataConnector =
      DataConnectorFactory.getDirectDataConnector(sqlContext, ssc, userParam.targetParam,
        ruleAnalyzer.targetRuleExprs, finalConstMap
      ) match {
        case Success(cntr) => {
          if (cntr.available) cntr
          else throw new Exception("target data connection error!")
        }
        case Failure(ex) => throw ex
      }

    val cacheResultProcesser = CacheResultProcesser()

    // init data stream
    sourceDataConnector.init()
    targetDataConnector.init()

    // my algo
    val algo = StreamingAccuracyAlgo(allParam)

    // process thread
    case class Process() extends Runnable {
      val lock = InfoCacheInstance.genLock("process")
      def run(): Unit = {
        val updateTime = new Date().getTime
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

            println(s"sourceData.count: ${sourceData.count}")
            println(s"targetData.count: ${targetData.count}")

            // accuracy algorithm
            val (accuResult, missingRdd, matchedRdd) = algo.accuracy(sourceData, targetData, ruleAnalyzer)
            println(accuResult)

            val ct = new Date().getTime
            appPersist.log(ct, s"calculation using time: ${ct - st} ms")

            // result of every group
            val matchedGroups = algo.reorgByTimeGroup(matchedRdd)
            val matchedGroupCount = matchedGroups.count
            println(s"===== matchedGroupCount: ${matchedGroupCount} =====")

            // get missing results
            val missingGroups = algo.reorgByTimeGroup(missingRdd)
            val missingGroupCount = missingGroups.count
            println(s"===== missingGroupCount: ${missingGroupCount} =====")

            val groups = matchedGroups.cogroup(missingGroups)
            val groupCount = groups.count
            println(s"===== groupCount: ${groupCount} =====")

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
              println(s"update result: ${updateResult}")
              cacheResultProcesser.update(updateResult)
              // persist result
              val persist: Persist = persistFactory.getPersists(updateResult.timeGroup)
              persist.result(updateTime, updateResult.result)
            }

            // record missing data and update old data (in executor)
            updateDataPart.foreach { grp =>
              val (t, datas) = grp
              val persist: Persist = persistFactory.getPersists(t)
              // persist missing data
              val missStrings = datas.map { row =>
                val (_, (value, info)) = row
                s"${value} [${info.getOrElse(MismatchInfo.key, "unknown")}]"
              }
              persist.records(missStrings, PersistType.MISS)
              // data connector update old data
              val dumpDatas = datas.map { r =>
                val (_, (v, i)) = r
                v ++ i
              }

              println(t)
              dumpDatas.foreach(println)

              sourceDataConnector.updateOldData(t, dumpDatas)
              targetDataConnector.updateOldData(t, dumpDatas)    // not correct
            }

            updateResults.unpersist()

            // dump missing rdd   (this part not need for future version, only for current df cache data version)
            val dumpRdd: RDD[Map[String, Any]] = missingRdd.map { r =>
              val (_, (v, i)) = r
              v ++ i
            }
            sourceDataConnector.updateAllOldData(dumpRdd)
            targetDataConnector.updateAllOldData(dumpRdd)    // not correct

            // fixme: 3. refactor data connector module


            TimeInfoCache.endTimeInfoCache

            val et = new Date().getTime
            appPersist.log(et, s"persist using time: ${et - ct} ms")

          } catch {
            case e: Throwable => error(s"process error: ${e.getMessage}")
          } finally {
            lock.unlock()
          }
        }
      }
    }

    val processInterval = TimeUtil.milliseconds(sparkParam.processInterval) match {
      case Some(interval) => interval
      case _ => throw new Exception("invalid batch interval")
    }
    val process = TimingProcess(processInterval, Process())

    // clean thread
//    case class Clean() extends Runnable {
//      val lock = InfoCacheInstance.genLock("clean")
//      def run(): Unit = {
//        val locked = lock.lock(5, TimeUnit.SECONDS)
//        if (locked) {
//          try {
//            sourceDataConnector.cleanData
//            targetDataConnector.cleanData
//          } finally {
//            lock.unlock()
//          }
//        }
//      }
//    }
//    val cleanInterval = TimeUtil.milliseconds(cleanerParam.cleanInterval) match {
//      case Some(interval) => interval
//      case _ => throw new Exception("invalid batch interval")
//    }
//    val clean = TimingProcess(cleanInterval, Clean())

    process.startup()
//    clean.startup()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true, stopGracefully=true)

    // context stop
    sc.stop

    InfoCacheInstance.close

    appPersist.finish()

    process.shutdown()
//    clean.shutdown()
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
