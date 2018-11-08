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
package org.apache.griffin.measure.datasource.connector

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.configuration.enums.BatchProcessType
import org.apache.griffin.measure.context.{ContextId, DQContext, TimeRange}
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.job.builder.DQJobBuilder
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.step.builder.preproc.PreProcParamMaker

trait DataConnector extends Loggable with Serializable {

  val sparkSession: SparkSession

  val dcParam: DataConnectorParam

  val id: String = DataConnectorIdGenerator.genId

  val timestampStorage: TimestampStorage
  protected def saveTmst(t: Long) = timestampStorage.insert(t)
  protected def readTmst(t: Long) = timestampStorage.fromUntil(t, t + 1)

  def init(): Unit

  // get data frame in batch mode
  def data(ms: Long): (Option[DataFrame], TimeRange)

  private def createContext(t: Long): DQContext = {
    DQContext(ContextId(t, id), id, Nil, Nil, BatchProcessType)(sparkSession)
  }

  def preProcess(dfOpt: Option[DataFrame], ms: Long): Option[DataFrame] = {
    // new context
    val context = createContext(ms)

    val timestamp = context.contextId.timestamp
    val suffix = context.contextId.id
    val dcDfName = dcParam.getDataFrameName("this")

    try {
      saveTmst(timestamp)    // save timestamp

      dfOpt.flatMap { df =>
        val (preProcRules, thisTable) =
          PreProcParamMaker.makePreProcRules(dcParam.getPreProcRules, suffix, dcDfName)

        // init data
        context.compileTableRegister.registerTable(thisTable)
        context.runTimeTableRegister.registerTable(thisTable, df)

        // build job
        val preprocJob = DQJobBuilder.buildDQJob(context, preProcRules)

        // job execute
        preprocJob.execute(context)

        // out data
        val outDf = context.sparkSession.table(s"`${thisTable}`")

        // add tmst column
        val withTmstDf = outDf.withColumn(ConstantColumns.tmst, lit(timestamp))

        // clean context
        context.clean()

        Some(withTmstDf)
      }

    } catch {
      case e: Throwable =>
        error(s"pre-process of data connector [${id}] error: ${e.getMessage}", e)
        None
    }
  }
}

object DataConnectorIdGenerator {
  private val counter: AtomicLong = new AtomicLong(0L)
  private val head: String = "dc"

  def genId: String = {
    s"${head}${increment}"
  }

  private def increment: Long = {
    counter.incrementAndGet()
  }
}
