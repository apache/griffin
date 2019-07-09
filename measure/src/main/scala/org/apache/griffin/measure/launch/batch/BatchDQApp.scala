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
package org.apache.griffin.measure.launch.batch

import java.util.Date

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SQLContext}

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.context._
import org.apache.griffin.measure.datasource.DataSourceFactory
import org.apache.griffin.measure.job.builder.DQJobBuilder
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.step.builder.udf.GriffinUDFAgent


case class BatchDQApp(allParam: GriffinConfig) extends DQApp {

  val envParam: EnvConfig = allParam.getEnvConfig
  val dqParam: DQConfig = allParam.getDqConfig

  val sparkParam = envParam.getSparkParam
  val metricName = dqParam.getName
//  val dataSourceParams = dqParam.dataSources
//  val dataSourceNames = dataSourceParams.map(_.name)
  val sinkParams = getSinkParams

  var sqlContext: SQLContext = _

  implicit var sparkSession: SparkSession = _

  def retryable: Boolean = false

  def init: Try[_] = Try {
    // build spark 2.0+ application context
    val conf = new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.getConfig)
    conf.set("spark.sql.crossJoin.enabled", "true")
    sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    var logLevel = getGriffinLogLevel()
    sparkSession.sparkContext.setLogLevel(sparkParam.getLogLevel)
    griffinLogger.setLevel(logLevel)
    sqlContext = sparkSession.sqlContext

    // register udf
    GriffinUDFAgent.register(sqlContext)
  }

  def run: Try[Boolean] = Try {
    // start time
    val startTime = new Date().getTime

    val measureTime = getMeasureTime
    val contextId = ContextId(measureTime)

    // get data sources
    val dataSources = DataSourceFactory.getDataSources(sparkSession, null, dqParam.getDataSources)
    dataSources.foreach(_.init)

    // create dq context
    val dqContext: DQContext = DQContext(
      contextId, metricName, dataSources, sinkParams, BatchProcessType
    )(sparkSession)

    // start id
    val applicationId = sparkSession.sparkContext.applicationId
    dqContext.getSink().start(applicationId)

    // build job
    val dqJob = DQJobBuilder.buildDQJob(dqContext, dqParam.getEvaluateRule)

    // dq job execute
    val result = dqJob.execute(dqContext)

    // end time
    val endTime = new Date().getTime
    dqContext.getSink().log(endTime, s"process using time: ${endTime - startTime} ms")

    // clean context
    dqContext.clean()

    // finish
    dqContext.getSink().finish()

    result
  }

  def close: Try[_] = Try {
    sparkSession.close()
    sparkSession.stop()
  }

}
