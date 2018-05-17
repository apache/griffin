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

import org.apache.griffin.measure.cache.info.InfoCacheInstance
import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.data.source.DataSourceFactory
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.engine.DqEngineFactory
import org.apache.griffin.measure.process.temp.{DataFrameCaches, TableRegisters}
import org.apache.griffin.measure.rule.adaptor.RuleAdaptorGroup
import org.apache.griffin.measure.rule.udf._
import org.apache.griffin.measure.utils.{HdfsUtil, TimeUtil}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

case class StreamingDqProcess(allParam: AllParam) extends DqProcess {

  val envParam: EnvParam = allParam.envParam
  val userParam: UserParam = allParam.userParam

  val sparkParam = envParam.sparkParam
  val metricName = userParam.name
  val dataSourceNames = userParam.dataSources.map(_.name)
  val baselineDsName = userParam.baselineDsName

//  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _

  var sparkSession: SparkSession = _

  def retriable: Boolean = true

  def init: Try[_] = Try {
    val conf = new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.config)
    conf.set("spark.sql.crossJoin.enabled", "true")
    sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    sparkSession.sparkContext.setLogLevel(sparkParam.logLevel)
    sqlContext = sparkSession.sqlContext

    // clear checkpoint directory
    clearCpDir

    // init info cache instance
    InfoCacheInstance.initInstance(envParam.infoCacheParams, metricName)
    InfoCacheInstance.init

    // register udf
    GriffinUdfs.register(sqlContext)
    GriffinUdafs.register(sqlContext)

    // init adaptors
    val dataSourceNames = userParam.dataSources.map(_.name)
    RuleAdaptorGroup.init(sparkSession, dataSourceNames, baselineDsName)
  }

  def run: Try[_] = Try {
    val ssc = StreamingContext.getOrCreate(sparkParam.cpDir, () => {
      try {
        createStreamingContext
      } catch {
        case e: Throwable => {
          error(s"create streaming context error: ${e.getMessage}")
          throw e
        }
      }
    })

    // start time
    val appTime = getAppTime

    // get persists to persist measure result
    val persistFactory = PersistFactory(envParam.persistParams, metricName)
    val persist: Persist = persistFactory.getPersists(appTime)

    // persist start id
    val applicationId = sparkSession.sparkContext.applicationId
    persist.start(applicationId)

    // get dq engines
    val dqEngines = DqEngineFactory.genDqEngines(sqlContext)

    // generate data sources
    val dataSources = DataSourceFactory.genDataSources(sqlContext, ssc, dqEngines, userParam.dataSources)
    dataSources.foreach(_.init)

    // process thread
    val dqThread = StreamingDqThread(sqlContext, dqEngines, dataSources,
      userParam.evaluateRuleParam, persistFactory, persist)

    // init data sources
//    val dsTmsts = dqEngines.loadData(dataSources, appTime)

    // generate rule steps
//    val ruleSteps = RuleAdaptorGroup.genRuleSteps(
//      TimeInfo(appTime, appTime), userParam.evaluateRuleParam, dsTmsts)
//
//    // run rules
//    dqEngines.runRuleSteps(ruleSteps)
//
//    // persist results
//    dqEngines.persistAllResults(ruleSteps, persist)

    // end time
//    val endTime = new Date().getTime
//    persist.log(endTime, s"process using time: ${endTime - startTime} ms")

    val processInterval = TimeUtil.milliseconds(sparkParam.processInterval) match {
      case Some(interval) => interval
      case _ => throw new Exception("invalid batch interval")
    }
    val process = TimingProcess(processInterval, dqThread)
    process.startup()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true, stopGracefully=true)

    // finish
    persist.finish()

//    process.shutdown()
  }

  def end: Try[_] = Try {
    TableRegisters.unregisterCompileGlobalTables()
    TableRegisters.unregisterRunGlobalTables(sqlContext)

    DataFrameCaches.uncacheGlobalDataFrames()
    DataFrameCaches.clearGlobalTrashDataFrames()

    sparkSession.close()
    sparkSession.stop()

    InfoCacheInstance.close
  }

  def createStreamingContext: StreamingContext = {
    val batchInterval = TimeUtil.milliseconds(sparkParam.batchInterval) match {
      case Some(interval) => Milliseconds(interval)
      case _ => throw new Exception("invalid batch interval")
    }
    val ssc = new StreamingContext(sparkSession.sparkContext, batchInterval)
    ssc.checkpoint(sparkParam.cpDir)

    ssc
  }

  private def clearCpDir: Unit = {
    if (sparkParam.needInitClear) {
      val cpDir = sparkParam.cpDir
      println(s"clear checkpoint directory ${cpDir}")
      HdfsUtil.deleteHdfsPath(cpDir)
    }
  }

}
