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
package org.apache.griffin.measure.launch.streaming

import java.util.{Timer, TimerTask}
import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}

import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.configuration.params._
import org.apache.griffin.measure.context._
import org.apache.griffin.measure.context.datasource.DataSourceFactory
import org.apache.griffin.measure.context.streaming.info.InfoCacheInstance
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.step.builder.udf.GriffinUDFAgent
import org.apache.griffin.measure.utils.{HdfsUtil, TimeUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.util.Try

case class StreamingDQApp(allParam: AllParam) extends DQApp {

  val envParam: EnvParam = allParam.envParam
  val dqParam: DQParam = allParam.dqParam

  val sparkParam = envParam.sparkParam
  val metricName = dqParam.name
  val dataSourceParams = dqParam.dataSources
  val dataSourceNames = dataSourceParams.map(_.name)
  val persistParams = envParam.persistParams

  var sqlContext: SQLContext = _

  implicit var sparkSession: SparkSession = _

  def retryable: Boolean = true

  def init: Try[_] = Try {
    // build spark 2.0+ application context
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
    GriffinUDFAgent.register(sqlContext)
  }

  def run: Try[_] = Try {

    // streaming context
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
    val contextId = ContextId(appTime)

    // generate data sources
    val dataSources = DataSourceFactory.getDataSources(sparkSession, ssc, dqParam.dataSources)
    dataSources.foreach(_.init)

    // create dq context
    val globalContext: DQContext = DQContext(
      contextId, metricName, dataSources, persistParams, StreamingProcessType
    )(sparkSession)

    // start id
    val applicationId = sparkSession.sparkContext.applicationId
    globalContext.getPersist().start(applicationId)

    // process thread
    val dqThread = StreamingDQApp2(globalContext, dqParam.evaluateRule)

    val processInterval = TimeUtil.milliseconds(sparkParam.processInterval) match {
      case Some(interval) => interval
      case _ => throw new Exception("invalid batch interval")
    }
    val process = TimingProcess(processInterval, dqThread)
    process.startup()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true, stopGracefully=true)

    // clean context
    globalContext.clean()

    // finish
    globalContext.getPersist().finish()

  }

  def close: Try[_] = Try {
    sparkSession.close()
    sparkSession.stop()
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
      info(s"clear checkpoint directory ${cpDir}")
      HdfsUtil.deleteHdfsPath(cpDir)
    }
  }

  case class TimingProcess(interval: Long, runnable: Runnable) {

    val pool: ThreadPoolExecutor = Executors.newFixedThreadPool(5).asInstanceOf[ThreadPoolExecutor]

    val timer = new Timer("process", true)

    val timerTask = new TimerTask() {
      override def run(): Unit = {
        pool.submit(runnable)
      }
    }

    def startup(): Unit = {
      timer.schedule(timerTask, interval, interval)
    }

    def shutdown(): Unit = {
      timer.cancel()
      pool.shutdown()
      pool.awaitTermination(10, TimeUnit.SECONDS)
    }

  }

}
