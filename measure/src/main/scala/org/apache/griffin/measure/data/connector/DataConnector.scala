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
import org.apache.griffin.measure.process.temp.TempTables
import org.apache.griffin.measure.process.temp.TempKeys._
import org.apache.griffin.measure.rule.adaptor.{PreProcPhase, RuleAdaptorGroup, RunPhase}
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.preproc.PreProcRuleGenerator
import org.apache.griffin.measure.rule.step.TimeInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}


trait DataConnector extends Loggable with Serializable {

//  def available(): Boolean

  var tmstCache: TmstCache = _
  protected def saveTmst(t: Long) = tmstCache.insert(t)
  protected def readTmst(t: Long) = tmstCache.range(t, t + 20)

  def init(): Unit

  def data(ms: Long): (Option[DataFrame], Set[Long])

  val dqEngines: DqEngines

  val dcParam: DataConnectorParam

  val sqlContext: SQLContext

  val id: String = DataConnectorIdGenerator.genId

  protected def suffix(ms: Long): String = s"${id}_${ms}"
  protected def thisName(ms: Long): String = s"this_${suffix(ms)}"

  final val tmstColName = InternalColumns.tmst

  def preProcess(dfOpt: Option[DataFrame], ms: Long): Option[DataFrame] = {
    val thisTable = thisName(ms)
    val preProcRules = PreProcRuleGenerator.genPreProcRules(dcParam.preProc, suffix(ms))
//    val names = PreProcRuleGenerator.getRuleNames(preProcRules).toSet + thisTable

    try {
      dfOpt.flatMap { df =>
        // in data
        TempTables.registerTempTable(df, key(id, ms), thisTable)

//        val dsTmsts = Map[String, Set[Long]]((thisTable -> Set[Long](ms)))
        val tmsts = Seq[Long](ms)

        // generate rule steps
        val ruleSteps = RuleAdaptorGroup.genRuleSteps(
          TimeInfo(ms, ms), preProcRules, tmsts, DslType("spark-sql"), PreProcPhase)

        // run rules
        dqEngines.runRuleSteps(ruleSteps)

        // out data
        val outDf = sqlContext.table(s"`${thisTable}`")

        // drop temp tables
        TempTables.unregisterTempTables(sqlContext, key(id, ms))
//        names.foreach { name =>
//          try {
//            TempTables.unregisterTempTable(sqlContext, ms, name)
//          } catch {
//            case e: Throwable => warn(s"drop temp table ${name} fails")
//          }
//        }

        val range = if (id == "dc1") (0 until 20).toList else (0 until 1).toList
        val withTmstDfs = range.map { i =>
          saveTmst(ms + i)
          outDf.withColumn(tmstColName, lit(ms + i)).limit(49 - i)
        }
        Some(withTmstDfs.reduce(_ unionAll _))

        // add tmst
//        val withTmstDf = outDf.withColumn(tmstColName, lit(ms))
//
//        // tmst cache
//        saveTmst(ms)
//
//        Some(withTmstDf)
      }
    } catch {
      case e: Throwable => {
        error(s"preporcess of data connector [${id}] error: ${e.getMessage}")
        None
      }
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

object InternalColumns {
  val tmst = "__tmst"
  val ignoreCache = "__ignoreCache"

  val columns = List[String](tmst, ignoreCache)

  def clearInternalColumns(v: Map[String, Any]): Map[String, Any] = {
    v -- columns
  }
}