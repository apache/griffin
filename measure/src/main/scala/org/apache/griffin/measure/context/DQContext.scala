/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.context

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums.ProcessType._
import org.apache.griffin.measure.configuration.enums.WriteMode
import org.apache.griffin.measure.datasource._
import org.apache.griffin.measure.sink.{Sink, SinkFactory}

/**
 * dq context: the context of each calculation
 * unique context id in each calculation
 * access the same spark session this app created
 */
case class DQContext(
    contextId: ContextId,
    name: String,
    dataSources: Seq[DataSource],
    sinkParams: Seq[SinkParam],
    procType: ProcessType)(@transient implicit val sparkSession: SparkSession) {

  val compileTableRegister: CompileTableRegister = CompileTableRegister()
  val runTimeTableRegister: RunTimeTableRegister = RunTimeTableRegister(sparkSession)

  val dataFrameCache: DataFrameCache = DataFrameCache()

  val metricWrapper: MetricWrapper = MetricWrapper(name, sparkSession.sparkContext.applicationId)
  val writeMode: WriteMode = WriteMode.defaultMode(procType)

  val dataSourceNames: Seq[String] = {
    // sort data source names, put baseline data source name to the head
    val (blOpt, others) = dataSources.foldLeft((None: Option[String], Nil: Seq[String])) {
      (ret, ds) =>
        val (opt, seq) = ret
        if (opt.isEmpty && ds.isBaseline) (Some(ds.name), seq) else (opt, seq :+ ds.name)
    }
    blOpt match {
      case Some(bl) => bl +: others
      case _ => others
    }
  }
  dataSourceNames.foreach(name => compileTableRegister.registerTable(name))

  def getDataSourceName(index: Int): String = {
    if (dataSourceNames.size > index) dataSourceNames(index) else ""
  }

  implicit val encoder: Encoder[String] = Encoders.STRING
  val functionNames: Seq[String] = sparkSession.catalog.listFunctions.map(_.name).collect.toSeq

  val dataSourceTimeRanges: Map[String, TimeRange] = loadDataSources()

  def loadDataSources(): Map[String, TimeRange] = {
    dataSources.map { ds =>
      (ds.name, ds.loadData(this))
    }.toMap
  }

  printTimeRanges()

  private val sinkFactory = SinkFactory(sinkParams, name)
  private val defaultSink: Sink = createSink(contextId.timestamp)

  def getSink(timestamp: Long): Sink = {
    if (timestamp == contextId.timestamp) getSink
    else createSink(timestamp)
  }

  def getSink: Sink = defaultSink

  private def createSink(t: Long): Sink = {
    procType match {
      case BatchProcessType => sinkFactory.getSinks(t, block = true)
      case StreamingProcessType => sinkFactory.getSinks(t, block = false)
    }
  }

  def cloneDQContext(newContextId: ContextId): DQContext = {
    DQContext(newContextId, name, dataSources, sinkParams, procType)(sparkSession)
  }

  def clean(): Unit = {
    compileTableRegister.unregisterAllTables()
    runTimeTableRegister.unregisterAllTables()

    dataFrameCache.uncacheAllDataFrames()
    dataFrameCache.clearAllTrashDataFrames()
  }

  private def printTimeRanges(): Unit = {
    if (dataSourceTimeRanges.nonEmpty) {
      val timeRangesStr = dataSourceTimeRanges
        .map { pair =>
          val (name, timeRange) = pair
          s"$name -> (${timeRange.begin}, ${timeRange.end}]"
        }
        .mkString(", ")
      println(s"data source timeRanges: $timeRangesStr")
    }
  }

}
