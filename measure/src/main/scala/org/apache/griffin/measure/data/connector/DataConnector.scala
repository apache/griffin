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

import org.apache.griffin.measure.config.params.user.DataConnectorParam
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.engine._
import org.apache.griffin.measure.rule.adaptor.{PreProcPhase, RuleAdaptorGroup, RunPhase}
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.preproc.PreProcRuleGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}


trait DataConnector extends Loggable with Serializable {

//  def available(): Boolean

  def init(): Unit

  def data(ms: Long): Option[DataFrame]

  val dqEngines: DqEngines

  val dcParam: DataConnectorParam

  val sqlContext: SQLContext

  val id: String = DataConnectorIdGenerator.genId

  protected def suffix(ms: Long): String = s"${id}_${ms}"
  protected def thisName(ms: Long): String = s"this_${suffix(ms)}"

  final val tmstColName = GroupByColumn.tmst

  def preProcess(dfOpt: Option[DataFrame], ms: Long): Option[DataFrame] = {
    val thisTable = thisName(ms)
    val preProcRules = PreProcRuleGenerator.genPreProcRules(dcParam.preProc, suffix(ms))
    val names = PreProcRuleGenerator.getRuleNames(preProcRules).toSet + thisTable

    try {
      dfOpt.flatMap { df =>
        // in data
        df.registerTempTable(thisTable)

        // generate rule steps
        val ruleSteps = RuleAdaptorGroup.genConcreteRuleSteps(preProcRules, DslType("spark-sql"), PreProcPhase)

        // run rules
        dqEngines.runRuleSteps(ruleSteps)

        // out data
        val outDf = sqlContext.table(thisTable)

        // drop temp table
        names.foreach { name =>
          try {
            sqlContext.dropTempTable(name)
          } catch {
            case e: Throwable => warn(s"drop temp table ${name} fails")
          }
        }

        // add tmst
        val withTmstDf = outDf.withColumn(tmstColName, lit(ms))

        Some(withTmstDf)
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

object GroupByColumn {
  val tmst = "__tmst"
}