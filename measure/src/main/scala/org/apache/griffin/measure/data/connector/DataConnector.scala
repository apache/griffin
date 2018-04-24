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
package org.apache.griffin.measure.data.connector

import java.util.concurrent.atomic.AtomicLong

import org.apache.griffin.measure.cache.tmst.TmstCache
import org.apache.griffin.measure.config.params.user.DataConnectorParam
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.{BatchDqProcess, BatchProcessType}
import org.apache.griffin.measure.process.engine._
import org.apache.griffin.measure.process.temp.{DataFrameCaches, TableRegisters, TimeRange}
import org.apache.griffin.measure.rule.adaptor.{InternalColumns, PreProcPhase, RuleAdaptorGroup, RunPhase}
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.rule.preproc.PreProcRuleGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}


trait DataConnector extends Loggable with Serializable {

//  def available(): Boolean

  var tmstCache: TmstCache = _
  protected def saveTmst(t: Long) = tmstCache.insert(t)
  protected def readTmst(t: Long) = tmstCache.range(t, t + 1)

  def init(): Unit

  def data(ms: Long): (Option[DataFrame], TimeRange)

  val dqEngines: DqEngines

  val dcParam: DataConnectorParam

  val sqlContext: SQLContext

  val id: String = DataConnectorIdGenerator.genId

  protected def suffix(ms: Long): String = s"${id}_${ms}"
  protected def thisName(ms: Long): String = s"this_${suffix(ms)}"

  final val tmstColName = InternalColumns.tmst

  def preProcess(dfOpt: Option[DataFrame], ms: Long): Option[DataFrame] = {
    val timeInfo = CalcTimeInfo(ms, id)
    val thisTable = thisName(ms)

    try {
      saveTmst(ms)    // save tmst

      dfOpt.flatMap { df =>
        val preProcRules = PreProcRuleGenerator.genPreProcRules(dcParam.preProc, suffix(ms))

        // init data
        TableRegisters.registerRunTempTable(df, timeInfo.key, thisTable)

//        val dsTmsts = Map[String, Set[Long]]((thisTable -> Set[Long](ms)))
//        val tmsts = Seq[Long](ms)
        val dsTimeRanges = Map[String, TimeRange]((thisTable -> TimeRange(ms)))

        // generate rule steps
        val rulePlan = RuleAdaptorGroup.genRulePlan(
          timeInfo, preProcRules, SparkSqlType, BatchProcessType, dsTimeRanges)

        // run rules
        dqEngines.runRuleSteps(timeInfo, rulePlan.ruleSteps)

        // out data
        val outDf = sqlContext.table(s"`${thisTable}`")

//        names.foreach { name =>
//          try {
//            TempTables.unregisterTempTable(sqlContext, ms, name)
//          } catch {
//            case e: Throwable => warn(s"drop temp table ${name} fails")
//          }
//        }

//        val range = if (id == "dc1") (0 until 20).toList else (0 until 1).toList
//        val withTmstDfs = range.map { i =>
//          saveTmst(ms + i)
//          outDf.withColumn(tmstColName, lit(ms + i)).limit(49 - i)
//        }
//        Some(withTmstDfs.reduce(_ unionAll _))

        // add tmst column
        val withTmstDf = outDf.withColumn(tmstColName, lit(ms))

        // tmst cache
//        saveTmst(ms)

        // drop temp tables
        cleanData(timeInfo)

        Some(withTmstDf)
      }
    } catch {
      case e: Throwable => {
        error(s"pre-process of data connector [${id}] error: ${e.getMessage}")
        None
      }
    }

  }

  private def cleanData(timeInfo: TimeInfo): Unit = {
    TableRegisters.unregisterRunTempTables(sqlContext, timeInfo.key)
    TableRegisters.unregisterCompileTempTables(timeInfo.key)

    DataFrameCaches.uncacheDataFrames(timeInfo.key)
    DataFrameCaches.clearTrashDataFrames(timeInfo.key)
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

