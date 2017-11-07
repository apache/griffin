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

import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.data.source.DataSourceFactory
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.engine.{DqEngineFactory, SparkSqlEngine}
import org.apache.griffin.measure.rule.adaptor.{RuleAdaptorGroup, RunPhase}
import org.apache.griffin.measure.rule.udf.GriffinUdfs
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

case class BatchDqProcess(allParam: AllParam) extends DqProcess {

  val envParam: EnvParam = allParam.envParam
  val userParam: UserParam = allParam.userParam

  val metricName = userParam.name
  val sparkParam = envParam.sparkParam

  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _

  def retriable: Boolean = false

  def init: Try[_] = Try {
    val conf = new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.config)
    sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel(sparkParam.logLevel)
    sqlContext = new HiveContext(sparkContext)

    // register udf
    GriffinUdfs.register(sqlContext)

    // init adaptors
    val dataSourceNames = userParam.dataSources.map(_.name)
    RuleAdaptorGroup.init(sqlContext, dataSourceNames)
  }

  def run: Try[_] = Try {
    // start time
    val startTime = getStartTime

    // get persists to persist measure result
    val persistFactory = PersistFactory(envParam.persistParams, metricName)
    val persist: Persist = persistFactory.getPersists(startTime)

    // persist start id
    val applicationId = sparkContext.applicationId
    persist.start(applicationId)

    // get dq engines
    val dqEngines = DqEngineFactory.genDqEngines(sqlContext)

    // generate data sources
    val dataSources = DataSourceFactory.genDataSources(sqlContext, null, dqEngines, userParam.dataSources, metricName)
    dataSources.foreach(_.init)

    // init data sources
    dqEngines.loadData(dataSources, startTime)

    // generate rule steps
    val ruleSteps = RuleAdaptorGroup.genConcreteRuleSteps(userParam.evaluateRuleParam, RunPhase)

    // run rules
    dqEngines.runRuleSteps(ruleSteps)

    // persist results
    val timeGroups = dqEngines.persistAllMetrics(ruleSteps, persistFactory)

    val rdds = dqEngines.collectUpdateRDDs(ruleSteps, timeGroups)
    rdds.foreach(_._2.cache())

    dqEngines.persistAllRecords(rdds, persistFactory)
//    dqEngines.persistAllRecords(ruleSteps, persistFactory, timeGroups)

    rdds.foreach(_._2.unpersist())

    // end time
    val endTime = new Date().getTime
    persist.log(endTime, s"process using time: ${endTime - startTime} ms")

    // finish
    persist.finish()
  }

  def end: Try[_] = Try {
    sparkContext.stop
  }

}
