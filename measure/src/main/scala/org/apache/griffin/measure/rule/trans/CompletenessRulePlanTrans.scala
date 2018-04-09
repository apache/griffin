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

import org.apache.griffin.measure.process.temp.TableRegisters
import org.apache.griffin.measure.process.{BatchProcessType, ExportMode, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.adaptor.RuleParamKeys._
import org.apache.griffin.measure.rule.adaptor._
import org.apache.griffin.measure.rule.dsl.analyzer.CompletenessAnalyzer
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.rule.trans.RuleExportFactory._
import org.apache.griffin.measure.utils.ParamUtil._

import scala.util.Try

case class CompletenessRulePlanTrans(dataSourceNames: Seq[String],
                                     timeInfo: TimeInfo, name: String, expr: Expr,
                                     param: Map[String, Any], procType: ProcessType
                                    ) extends RulePlanTrans {

  private object CompletenessKeys {
    val _source = "source"
    val _total = "total"
    val _complete = "complete"
    val _incomplete = "incomplete"
  }
  import CompletenessKeys._

  def trans(): Try[RulePlan] =  Try {
    val details = getDetails(param)
    val completenessClause = expr.asInstanceOf[CompletenessClause]
    val sourceName = details.getString(_source, dataSourceNames.head)

    val mode = ExportMode.defaultMode(procType)

    val ct = timeInfo.calcTime

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      emptyRulePlan
    } else {
      val analyzer = CompletenessAnalyzer(completenessClause, sourceName)

      val selItemsClause = analyzer.selectionPairs.map { pair =>
        val (expr, alias) = pair
        s"${expr.desc} AS `${alias}`"
      }.mkString(", ")
      val aliases = analyzer.selectionPairs.map(_._2)

      val selClause = procType match {
        case BatchProcessType => selItemsClause
        case StreamingProcessType => s"`${InternalColumns.tmst}`, ${selItemsClause}"
      }
      val selAliases = procType match {
        case BatchProcessType => aliases
        case StreamingProcessType => InternalColumns.tmst +: aliases
      }

      // 1. source alias
      val sourceAliasTableName = "__sourceAlias"
      val sourceAliasSql = {
        s"SELECT ${selClause} FROM `${sourceName}`"
      }
      val sourceAliasStep = SparkSqlStep(sourceAliasTableName, sourceAliasSql, emptyMap, true)

      // 2. incomplete record
      val incompleteRecordsTableName = "__incompleteRecords"
      val completeWhereClause = aliases.map(a => s"`${a}` IS NOT NULL").mkString(" AND ")
      val incompleteWhereClause = s"NOT (${completeWhereClause})"
      val incompleteRecordsSql = s"SELECT * FROM `${sourceAliasTableName}` WHERE ${incompleteWhereClause}"
      val incompleteRecordStep = SparkSqlStep(incompleteRecordsTableName, incompleteRecordsSql, emptyMap, true)
      val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
      val incompleteRecordExport = genRecordExport(recordParam, incompleteRecordsTableName, incompleteRecordsTableName, ct, mode)

      // 3. incomplete count
      val incompleteCountTableName = "__missCount"
      val incompleteColName = details.getStringOrKey(_incomplete)
      val incompleteCountSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${incompleteColName}` FROM `${incompleteRecordsTableName}`"
        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${incompleteCountTableName}` FROM `${incompleteRecordsTableName}` GROUP BY `${InternalColumns.tmst}`"
      }
      val incompleteCountStep = SparkSqlStep(incompleteCountTableName, incompleteCountSql, emptyMap)

      // 4. total count
      val totalCountTableName = "__totalCount"
      val totalColName = details.getStringOrKey(_total)
      val totalCountSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceAliasTableName}`"
        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceAliasTableName}` GROUP BY `${InternalColumns.tmst}`"
      }
      val totalCountStep = SparkSqlStep(totalCountTableName, totalCountSql, emptyMap)

      // 5. complete metric
      val completeTableName = name
      val completeColName = details.getStringOrKey(_complete)
      val completeMetricSql = procType match {
        case BatchProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${incompleteCountTableName}`.`${incompleteColName}`, 0) AS `${incompleteColName}`,
             |(`${totalColName}` - `${incompleteColName}`) AS `${completeColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${incompleteCountTableName}`
         """.stripMargin
        }
        case StreamingProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${incompleteCountTableName}`.`${incompleteColName}`, 0) AS `${incompleteColName}`,
             |(`${totalColName}` - `${incompleteColName}`) AS `${completeColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${incompleteCountTableName}`
             |ON `${totalCountTableName}`.`${InternalColumns.tmst}` = `${incompleteCountTableName}`.`${InternalColumns.tmst}`
         """.stripMargin
        }
      }
      val completeStep = SparkSqlStep(completeTableName, completeMetricSql, emptyMap)
      val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
      val completeExport = genMetricExport(metricParam, completeTableName, completeTableName, ct, mode)

      // complete plan
      val completeSteps = sourceAliasStep :: incompleteRecordStep :: incompleteCountStep :: totalCountStep :: completeStep :: Nil
      val completeExports = incompleteRecordExport :: completeExport :: Nil
      val completePlan = RulePlan(completeSteps, completeExports)

      completePlan
    }
  }

}
