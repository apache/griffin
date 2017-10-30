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

import org.apache.griffin.measure.cache.info.InfoCacheInstance
import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.data.source.DataSourceFactory
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.engine.DqEngineFactory
import org.apache.griffin.measure.rule.adaptor.RuleAdaptorGroup
import org.apache.griffin.measure.rule.udf.GriffinUdfs
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

case class StreamingDqProcess(allParam: AllParam) extends DqProcess {

  val envParam: EnvParam = allParam.envParam
  val userParam: UserParam = allParam.userParam

  val metricName = userParam.name
  val sparkParam = envParam.sparkParam

  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _

  def retriable: Boolean = true

  def init: Try[_] = Try {
    val conf = new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.config)
    sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel(sparkParam.logLevel)
    sqlContext = new HiveContext(sparkContext)

    // init info cache instance
    InfoCacheInstance.initInstance(envParam.infoCacheParams, metricName)
    InfoCacheInstance.init

    // register udf
    GriffinUdfs.register(sqlContext)

    // init adaptors
    val dataSourceNames = userParam.dataSources.map(_.name)
    RuleAdaptorGroup.init(sqlContext, dataSourceNames)
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
    val startTime = new Date().getTime()

    // get persists to persist measure result
    val persistFactory = PersistFactory(envParam.persistParams, metricName)
    val persist: Persist = persistFactory.getPersists(startTime)

    // persist start id
    val applicationId = sparkContext.applicationId
    persist.start(applicationId)

    // get dq engines
    val dqEngines = DqEngineFactory.genDqEngines(sqlContext)

    // generate data sources
    val dataSources = DataSourceFactory.genDataSources(sqlContext, ssc, dqEngines, userParam.dataSources, metricName)
    dataSources.foreach(_.init)

    // process thread
    val dqThread = StreamingDqThread(dqEngines, dataSources, userParam.evaluateRuleParam, persistFactory, persist)

    // init data sources
//    dqEngines.loadData(dataSources)
//
//    // generate rule steps
//    val ruleSteps = RuleAdaptorGroup.genConcreteRuleSteps(userParam.evaluateRuleParam)
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
    sparkContext.stop

    InfoCacheInstance.close
  }

  def createStreamingContext: StreamingContext = {
    val batchInterval = TimeUtil.milliseconds(sparkParam.batchInterval) match {
      case Some(interval) => Milliseconds(interval)
      case _ => throw new Exception("invalid batch interval")
    }
    val ssc = new StreamingContext(sparkContext, batchInterval)
    ssc.checkpoint(sparkParam.cpDir)



    ssc
  }

}
