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

import org.apache.griffin.measure.process._
import org.apache.griffin.measure.process.temp._
import org.apache.griffin.measure.rule.adaptor.RuleParamKeys._
import org.apache.griffin.measure.rule.adaptor._
import org.apache.griffin.measure.rule.dsl.analyzer.UniquenessAnalyzer
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.rule.trans.RuleExportFactory._
import org.apache.griffin.measure.utils.ParamUtil._

import scala.util.Try

case class UniquenessRulePlanTrans(dataSourceNames: Seq[String],
                                   timeInfo: TimeInfo, name: String, expr: Expr,
                                   param: Map[String, Any], procType: ProcessType
                                  ) extends RulePlanTrans {

  private object UniquenessKeys {
    val _source = "source"
    val _target = "target"
    val _unique = "unique"
    val _total = "total"
    val _dup = "dup"
    val _num = "num"

    val _duplicationArray = "duplication.array"
  }
  import UniquenessKeys._

  def trans(): Try[RulePlan] = Try {
    val details = getDetails(param)
    val sourceName = details.getString(_source, dataSourceNames.head)
    val targetName = details.getString(_target, dataSourceNames.tail.head)
    val analyzer = UniquenessAnalyzer(expr.asInstanceOf[UniquenessClause], sourceName, targetName)

    val mode = ExportMode.defaultMode(procType)

    val ct = timeInfo.calcTime

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      println(s"[${ct}] data source ${sourceName} not exists")
      emptyRulePlan
    } else if (!TableRegisters.existRunTempTable(timeInfo.key, targetName)) {
      println(s"[${ct}] data source ${targetName} not exists")
      emptyRulePlan
    } else {
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

      // 1. source distinct mapping
      val sourceTableName = "__source"
      val sourceSql = s"SELECT DISTINCT ${selClause} FROM ${sourceName}"
      val sourceStep = SparkSqlStep(sourceTableName, sourceSql, emptyMap)

      // 2. target mapping
      val targetTableName = "__target"
      val targetSql = s"SELECT ${selClause} FROM ${targetName}"
      val targetStep = SparkSqlStep(targetTableName, targetSql, emptyMap)

      // 3. joined
      val joinedTableName = "__joined"
      val joinedSelClause = selAliases.map { alias =>
        s"`${sourceTableName}`.`${alias}` AS `${alias}`"
      }.mkString(", ")
      val onClause = aliases.map { alias =>
        s"coalesce(`${sourceTableName}`.`${alias}`, '') = coalesce(`${targetTableName}`.`${alias}`, '')"
      }.mkString(" AND ")
      val joinedSql = {
        s"SELECT ${joinedSelClause} FROM `${targetTableName}` RIGHT JOIN `${sourceTableName}` ON ${onClause}"
      }
      val joinedStep = SparkSqlStep(joinedTableName, joinedSql, emptyMap)

      // 4. group
      val groupTableName = "__group"
      val groupSelClause = selAliases.map { alias =>
        s"`${alias}`"
      }.mkString(", ")
      val dupColName = details.getStringOrKey(_dup)
      val groupSql = {
        s"SELECT ${groupSelClause}, (COUNT(*) - 1) AS `${dupColName}` FROM `${joinedTableName}` GROUP BY ${groupSelClause}"
      }
      val groupStep = SparkSqlStep(groupTableName, groupSql, emptyMap, true)

      // 5. total metric
      val totalTableName = "__totalMetric"
      val totalColName = details.getStringOrKey(_total)
      val totalSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
        case StreamingProcessType => {
          s"""
             |SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}`
             |FROM `${sourceName}` GROUP BY `${InternalColumns.tmst}`
           """.stripMargin
        }
      }
      val totalStep = SparkSqlStep(totalTableName, totalSql, emptyMap)
      val totalMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
      val totalMetricExport = genMetricExport(totalMetricParam, totalColName, totalTableName, ct, mode)

      // 6. unique record
      val uniqueRecordTableName = "__uniqueRecord"
      val uniqueRecordSql = {
        s"SELECT * FROM `${groupTableName}` WHERE `${dupColName}` = 0"
      }
      val uniqueRecordStep = SparkSqlStep(uniqueRecordTableName, uniqueRecordSql, emptyMap)

      // 7. unique metric
      val uniqueTableName = "__uniqueMetric"
      val uniqueColName = details.getStringOrKey(_unique)
      val uniqueSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${uniqueColName}` FROM `${uniqueRecordTableName}`"
        case StreamingProcessType => {
          s"""
             |SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${uniqueColName}`
             |FROM `${uniqueRecordTableName}` GROUP BY `${InternalColumns.tmst}`
           """.stripMargin
        }
      }
      val uniqueStep = SparkSqlStep(uniqueTableName, uniqueSql, emptyMap)
      val uniqueMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
      val uniqueMetricExport = genMetricExport(uniqueMetricParam, uniqueColName, uniqueTableName, ct, mode)

      val uniqueSteps = sourceStep :: targetStep :: joinedStep :: groupStep ::
        totalStep :: uniqueRecordStep :: uniqueStep :: Nil
      val uniqueExports = totalMetricExport :: uniqueMetricExport :: Nil
      val uniqueRulePlan = RulePlan(uniqueSteps, uniqueExports)

      val duplicationArrayName = details.getString(_duplicationArray, "")
      val dupRulePlan = if (duplicationArrayName.nonEmpty) {
        // 8. duplicate record
        val dupRecordTableName = "__dupRecords"
        val dupRecordSql = {
          s"SELECT * FROM `${groupTableName}` WHERE `${dupColName}` > 0"
        }
        val dupRecordStep = SparkSqlStep(dupRecordTableName, dupRecordSql, emptyMap, true)
        val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
        val dupRecordExport = genRecordExport(recordParam, dupRecordTableName, dupRecordTableName, ct, mode)

        // 9. duplicate metric
        val dupMetricTableName = "__dupMetric"
        val numColName = details.getStringOrKey(_num)
        val dupMetricSelClause = procType match {
          case BatchProcessType => s"`${dupColName}`, COUNT(*) AS `${numColName}`"
          case StreamingProcessType => s"`${InternalColumns.tmst}`, `${dupColName}`, COUNT(*) AS `${numColName}`"
        }
        val dupMetricGroupbyClause = procType match {
          case BatchProcessType => s"`${dupColName}`"
          case StreamingProcessType => s"`${InternalColumns.tmst}`, `${dupColName}`"
        }
        val dupMetricSql = {
          s"""
             |SELECT ${dupMetricSelClause} FROM `${dupRecordTableName}`
             |GROUP BY ${dupMetricGroupbyClause}
          """.stripMargin
        }
        val dupMetricStep = SparkSqlStep(dupMetricTableName, dupMetricSql, emptyMap)
        val dupMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, ArrayCollectType.desc)
        val dupMetricExport = genMetricExport(dupMetricParam, duplicationArrayName, dupMetricTableName, ct, mode)

        RulePlan(dupRecordStep :: dupMetricStep :: Nil, dupRecordExport :: dupMetricExport :: Nil)
      } else emptyRulePlan

      uniqueRulePlan.merge(dupRulePlan)
    }
  }

}
