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
package org.apache.griffin.measure.step.builder.dsl.transform

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.step.builder.dsl.expr._
import org.apache.griffin.measure.step.builder.dsl.transform.analyzer.CompletenessAnalyzer
import org.apache.griffin.measure.step.transform.SparkSqlTransformStep
import org.apache.griffin.measure.step.write.{MetricWriteStep, RecordWriteStep}
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * generate completeness dq steps
  */
case class CompletenessExpr2DQSteps(context: DQContext,
                                    expr: Expr,
                                    ruleParam: RuleParam
                                   ) extends Expr2DQSteps {

  private object CompletenessKeys {
    val _source = "source"
    val _total = "total"
    val _complete = "complete"
    val _incomplete = "incomplete"
  }
  import CompletenessKeys._

  def getDQSteps(): Seq[DQStep] = {
    val details = ruleParam.getDetails
    val completenessExpr = expr.asInstanceOf[CompletenessClause]

    val sourceName = details.getString(_source, context.getDataSourceName(0))

    val procType = context.procType
    val timestamp = context.contextId.timestamp

    if (!context.runTimeTableRegister.existsTable(sourceName)) {
      warn(s"[${timestamp}] data source ${sourceName} not exists")
      Nil
    } else {
      val analyzer = CompletenessAnalyzer(completenessExpr, sourceName)

      val selItemsClause = analyzer.selectionPairs.map { pair =>
        val (expr, alias) = pair
        s"${expr.desc} AS `${alias}`"
      }.mkString(", ")
      val aliases = analyzer.selectionPairs.map(_._2)

      val selClause = procType match {
        case BatchProcessType => selItemsClause
        case StreamingProcessType => s"`${ConstantColumns.tmst}`, ${selItemsClause}"
      }
      val selAliases = procType match {
        case BatchProcessType => aliases
        case StreamingProcessType => ConstantColumns.tmst +: aliases
      }

      // 1. source alias
      val sourceAliasTableName = "__sourceAlias"
      val sourceAliasSql = {
        s"SELECT ${selClause} FROM `${sourceName}`"
      }
      val sourceAliasTransStep =
        SparkSqlTransformStep(sourceAliasTableName, sourceAliasSql, emptyMap, true)

      // 2. incomplete record
      val incompleteRecordsTableName = "__incompleteRecords"
      val completeWhereClause = aliases.map(a => s"`${a}` IS NOT NULL").mkString(" AND ")
      val incompleteWhereClause = s"NOT (${completeWhereClause})"

      val incompleteRecordsSql =
        s"SELECT * FROM `${sourceAliasTableName}` WHERE ${incompleteWhereClause}"

      val incompleteRecordTransStep =
        SparkSqlTransformStep(incompleteRecordsTableName, incompleteRecordsSql, emptyMap, true)
      incompleteRecordTransStep.parentSteps += sourceAliasTransStep
      val incompleteRecordWriteStep = {
        val rwName =
          ruleParam.getOutputOpt(RecordOutputType).flatMap(_.getNameOpt)
            .getOrElse(incompleteRecordsTableName)
        RecordWriteStep(rwName, incompleteRecordsTableName)
      }

      // 3. incomplete count
      val incompleteCountTableName = "__incompleteCount"
      val incompleteColName = details.getStringOrKey(_incomplete)
      val incompleteCountSql = procType match {
        case BatchProcessType =>
          s"SELECT COUNT(*) AS `${incompleteColName}` FROM `${incompleteRecordsTableName}`"
        case StreamingProcessType =>
          s"SELECT `${ConstantColumns.tmst}`, COUNT(*) AS `${incompleteColName}` " +
            s"FROM `${incompleteRecordsTableName}` GROUP BY `${ConstantColumns.tmst}`"
      }
      val incompleteCountTransStep =
        SparkSqlTransformStep(incompleteCountTableName, incompleteCountSql, emptyMap)
      incompleteCountTransStep.parentSteps += incompleteRecordTransStep

      // 4. total count
      val totalCountTableName = "__totalCount"
      val totalColName = details.getStringOrKey(_total)
      val totalCountSql = procType match {
        case BatchProcessType =>
          s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceAliasTableName}`"
        case StreamingProcessType =>
          s"SELECT `${ConstantColumns.tmst}`, COUNT(*) AS `${totalColName}` " +
            s"FROM `${sourceAliasTableName}` GROUP BY `${ConstantColumns.tmst}`"
      }
      val totalCountTransStep = SparkSqlTransformStep(totalCountTableName, totalCountSql, emptyMap)
      totalCountTransStep.parentSteps += sourceAliasTransStep

      // 5. complete metric
      val completeTableName = ruleParam.getOutDfName()
      val completeColName = details.getStringOrKey(_complete)
      val completeMetricSql = procType match {
        case BatchProcessType =>
          s"""
             |SELECT `${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${incompleteCountTableName}`.`${incompleteColName}`, 0) AS `${incompleteColName}`,
             |(`${totalCountTableName}`.`${totalColName}` - coalesce(`${incompleteCountTableName}`.`${incompleteColName}`, 0)) AS `${completeColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${incompleteCountTableName}`
         """.stripMargin
        case StreamingProcessType =>
          s"""
             |SELECT `${totalCountTableName}`.`${ConstantColumns.tmst}` AS `${ConstantColumns.tmst}`,
             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${incompleteCountTableName}`.`${incompleteColName}`, 0) AS `${incompleteColName}`,
             |(`${totalCountTableName}`.`${totalColName}` - coalesce(`${incompleteCountTableName}`.`${incompleteColName}`, 0)) AS `${completeColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${incompleteCountTableName}`
             |ON `${totalCountTableName}`.`${ConstantColumns.tmst}` = `${incompleteCountTableName}`.`${ConstantColumns.tmst}`
         """.stripMargin
      }
      val completeTransStep = SparkSqlTransformStep(completeTableName, completeMetricSql, emptyMap)
      completeTransStep.parentSteps += incompleteCountTransStep
      completeTransStep.parentSteps += totalCountTransStep
      val completeWriteStep = {
        val metricOpt = ruleParam.getOutputOpt(MetricOutputType)
        val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse(completeTableName)
        val flattenType = metricOpt.map(_.getFlatten).getOrElse(FlattenType.default)
        MetricWriteStep(mwName, completeTableName, flattenType)
      }

      val transSteps = completeTransStep :: Nil
      val writeSteps = incompleteRecordWriteStep :: completeWriteStep :: Nil

      // full steps
      transSteps ++ writeSteps
    }
  }

}
