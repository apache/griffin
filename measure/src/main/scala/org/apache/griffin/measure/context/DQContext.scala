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
package org.apache.griffin.measure.context

import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.datasource._
import org.apache.griffin.measure.sink.{Sink, SinkFactory}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}

/**
  * dq context: the context of each calculation
  * unique context id in each calculation
  * access the same spark session this app created
  */
case class DQContext(contextId: ContextId,
                     name: String,
                     dataSources: Seq[DataSource],
                     persistParams: Seq[PersistParam],
                     procType: ProcessType
                    )(@transient implicit val sparkSession: SparkSession) {

  val sqlContext: SQLContext = sparkSession.sqlContext

  val compileTableRegister: CompileTableRegister = CompileTableRegister()
  val runTimeTableRegister: RunTimeTableRegister = RunTimeTableRegister(sqlContext)

  val dataFrameCache: DataFrameCache = DataFrameCache()

  val metricWrapper: MetricWrapper = MetricWrapper(name)
  val writeMode = WriteMode.defaultMode(procType)

  val dataSourceNames: Seq[String] = dataSources.map(_.name)
  dataSourceNames.foreach(name => compileTableRegister.registerTable(name))
  implicit val encoder = Encoders.STRING
  val functionNames: Seq[String] = sparkSession.catalog.listFunctions.map(_.name).collect.toSeq

  val dataSourceTimeRanges = loadDataSources()
  def loadDataSources(): Map[String, TimeRange] = {
    dataSources.map { ds =>
      (ds.name, ds.loadData(this))
    }.toMap
  }
  printTimeRanges

  def getDataSourceName(index: Int): String = {
    if (dataSourceNames.size > index) dataSourceNames(index) else ""
  }

  private val persistFactory = SinkFactory(persistParams, name)
  private val defaultPersist: Sink = createPersist(contextId.timestamp)
  def getPersist(timestamp: Long): Sink = {
    if (timestamp == contextId.timestamp) getPersist()
    else createPersist(timestamp)
  }
  def getPersist(): Sink = defaultPersist
  private def createPersist(t: Long): Sink = {
    procType match {
      case BatchProcessType => persistFactory.getPersists(t, true)
      case StreamingProcessType => persistFactory.getPersists(t, false)
    }
  }

  def cloneDQContext(newContextId: ContextId): DQContext = {
    DQContext(newContextId, name, dataSources, persistParams, procType)(sparkSession)
  }

  def clean(): Unit = {
    compileTableRegister.unregisterAllTables()
    runTimeTableRegister.unregisterAllTables()

    dataFrameCache.uncacheAllDataFrames()
    dataFrameCache.clearAllTrashDataFrames()
  }

  private def printTimeRanges(): Unit = {
    if (dataSourceTimeRanges.nonEmpty) {
      val timeRangesStr = dataSourceTimeRanges.map { pair =>
        val (name, timeRange) = pair
        s"${name} -> (${timeRange.begin}, ${timeRange.end}]"
      }.mkString(", ")
      println(s"data source timeRanges: ${timeRangesStr}")
    }
  }

}
