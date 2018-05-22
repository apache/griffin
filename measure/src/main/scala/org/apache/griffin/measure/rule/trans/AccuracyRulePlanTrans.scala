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
package org.apache.griffin.measure.rule.trans

import org.apache.griffin.measure.process.engine.DataFrameOprs.AccuracyOprKeys
import org.apache.griffin.measure.process.temp.TableRegisters
import org.apache.griffin.measure.process.{BatchProcessType, ExportMode, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.adaptor._
import org.apache.griffin.measure.rule.adaptor.RuleParamKeys._
import org.apache.griffin.measure.rule.dsl.analyzer.AccuracyAnalyzer
import org.apache.griffin.measure.rule.dsl.expr.{Expr, LogicalExpr}
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.griffin.measure.rule.trans.RuleExportFactory._
import org.apache.griffin.measure.rule.trans.DsUpdateFactory._

import scala.util.Try

case class AccuracyRulePlanTrans(dataSourceNames: Seq[String],
                                 timeInfo: TimeInfo, name: String, expr: Expr,
                                 param: Map[String, Any], procType: ProcessType
                                ) extends RulePlanTrans {

  private object AccuracyKeys {
    val _source = "source"
    val _target = "target"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
  }
  import AccuracyKeys._

  def trans(): Try[RulePlan] = Try {
    val details = getDetails(param)
    val sourceName = details.getString(_source, dataSourceNames.head)
    val targetName = details.getString(_target, dataSourceNames.tail.head)
    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)

    val mode = ExportMode.defaultMode(procType)

    val ct = timeInfo.calcTime

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      println(s"[${ct}] data source ${sourceName} not exists")
      emptyRulePlan
    } else {
      // 1. miss record
      val missRecordsTableName = "__missRecords"
      val selClause = s"`${sourceName}`.*"
      val missRecordsSql = if (!TableRegisters.existRunTempTable(timeInfo.key, targetName)) {
        println(s"[${ct}] data source ${targetName} not exists")
        s"SELECT ${selClause} FROM `${sourceName}`"
      } else {
        val onClause = expr.coalesceDesc
        val sourceIsNull = analyzer.sourceSelectionExprs.map { sel =>
          s"${sel.desc} IS NULL"
        }.mkString(" AND ")
        val targetIsNull = analyzer.targetSelectionExprs.map { sel =>
          s"${sel.desc} IS NULL"
        }.mkString(" AND ")
        val whereClause = s"(NOT (${sourceIsNull})) AND (${targetIsNull})"
        s"SELECT ${selClause} FROM `${sourceName}` LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
      }
      val missRecordsStep = SparkSqlStep(missRecordsTableName, missRecordsSql, emptyMap, true)
      val missRecordsExports = procType match {
        case BatchProcessType => {
          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          genRecordExport(recordParam, missRecordsTableName, missRecordsTableName, ct, mode) :: Nil
        }
        case StreamingProcessType => Nil
      }
      val missRecordsUpdates = procType match {
        case BatchProcessType => Nil
        case StreamingProcessType => {
          val updateParam = emptyMap
          genDsUpdate(updateParam, sourceName, missRecordsTableName) :: Nil
        }
      }

      // 2. miss count
      val missCountTableName = "__missCount"
      val missColName = details.getStringOrKey(_miss)
      val missCountSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}`"
        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}` GROUP BY `${InternalColumns.tmst}`"
      }
      val missCountStep = SparkSqlStep(missCountTableName, missCountSql, emptyMap)

      // 3. total count
      val totalCountTableName = "__totalCount"
      val totalColName = details.getStringOrKey(_total)
      val totalCountSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceName}` GROUP BY `${InternalColumns.tmst}`"
      }
      val totalCountStep = SparkSqlStep(totalCountTableName, totalCountSql, emptyMap)

      // 4. accuracy metric
      val accuracyTableName = name
      val matchedColName = details.getStringOrKey(_matched)
      val accuracyMetricSql = procType match {
        case BatchProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
             |(`${totalCountTableName}`.`${totalColName}` - coalesce(`${missCountTableName}`.`${missColName}`, 0)) AS `${matchedColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
         """.stripMargin
        }
        case StreamingProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
             |(`${totalCountTableName}`.`${totalColName}` - coalesce(`${missCountTableName}`.`${missColName}`, 0)) AS `${matchedColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
             |ON `${totalCountTableName}`.`${InternalColumns.tmst}` = `${missCountTableName}`.`${InternalColumns.tmst}`
         """.stripMargin
        }
      }
      val accuracyStep = SparkSqlStep(accuracyTableName, accuracyMetricSql, emptyMap)
      val accuracyExports = procType match {
        case BatchProcessType => {
          val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
          genMetricExport(metricParam, accuracyTableName, accuracyTableName, ct, mode) :: Nil
        }
        case StreamingProcessType => Nil
      }

      // current accu plan
      val accuSteps = missRecordsStep :: missCountStep :: totalCountStep :: accuracyStep :: Nil
      val accuExports = missRecordsExports ++ accuracyExports
      val accuUpdates = missRecordsUpdates
      val accuPlan = RulePlan(accuSteps, accuExports, accuUpdates)

      // streaming extra accu plan
      val streamingAccuPlan = procType match {
        case BatchProcessType => emptyRulePlan
        case StreamingProcessType => {
          // 5. accuracy metric merge
          val accuracyMetricTableName = "__accuracy"
          val accuracyMetricRule = "accuracy"
          val accuracyMetricDetails = Map[String, Any](
            (AccuracyOprKeys._dfName -> accuracyTableName),
            (AccuracyOprKeys._miss -> missColName),
            (AccuracyOprKeys._total -> totalColName),
            (AccuracyOprKeys._matched -> matchedColName)
          )
          val accuracyMetricStep = DfOprStep(accuracyMetricTableName,
            accuracyMetricRule, accuracyMetricDetails)
          val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
          val accuracyMetricExports = genMetricExport(metricParam, name, accuracyMetricTableName, ct, mode) :: Nil

          // 6. collect accuracy records
          val accuracyRecordTableName = "__accuracyRecords"
          val accuracyRecordSql = {
            s"""
               |SELECT `${InternalColumns.tmst}`, `${InternalColumns.empty}`
               |FROM `${accuracyMetricTableName}` WHERE `${InternalColumns.record}`
             """.stripMargin
          }
          val accuracyRecordStep = SparkSqlStep(accuracyRecordTableName, accuracyRecordSql, emptyMap)
          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          val accuracyRecordParam = recordParam.addIfNotExist(ExportParamKeys._dataSourceCache, sourceName)
            .addIfNotExist(ExportParamKeys._originDF, missRecordsTableName)
          val accuracyRecordExports = genRecordExport(
            accuracyRecordParam, missRecordsTableName, accuracyRecordTableName, ct, mode) :: Nil

          // gen accu plan
          val extraSteps = accuracyMetricStep :: accuracyRecordStep :: Nil
          val extraExports = accuracyMetricExports ++ accuracyRecordExports
          val extraPlan = RulePlan(extraSteps, extraExports)

          extraPlan
        }
      }

      // return accu plan
      accuPlan.merge(streamingAccuPlan)

    }
  }

}
