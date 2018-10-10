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
import org.apache.griffin.measure.step.builder.dsl.transform.analyzer.AccuracyAnalyzer
import org.apache.griffin.measure.step.transform.{DataFrameOps, DataFrameOpsTransformStep, SparkSqlTransformStep}
import org.apache.griffin.measure.step.transform.DataFrameOps.AccuracyOprKeys
import org.apache.griffin.measure.step.write.{DataSourceUpdateWriteStep, MetricWriteStep, RecordWriteStep}
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * generate accuracy dq steps
  */
case class AccuracyExpr2DQSteps(context: DQContext,
                                expr: Expr,
                                ruleParam: RuleParam
                               ) extends Expr2DQSteps {

  private object AccuracyKeys {
    val _source = "source"
    val _target = "target"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
    val _matchedFraction = "matchedFraction"
  }
  import AccuracyKeys._

  def getDQSteps(): Seq[DQStep] = {
    val details = ruleParam.getDetails
    val accuracyExpr = expr.asInstanceOf[LogicalExpr]

    val sourceName = details.getString(_source, context.getDataSourceName(0))
    val targetName = details.getString(_target, context.getDataSourceName(1))
    val analyzer = AccuracyAnalyzer(accuracyExpr, sourceName, targetName)

    val procType = context.procType
    val timestamp = context.contextId.timestamp

    if (!context.runTimeTableRegister.existsTable(sourceName)) {
      warn(s"[${timestamp}] data source ${sourceName} not exists")
      Nil
    } else {
      // 1. miss record
      val missRecordsTableName = "__missRecords"
      val selClause = s"`${sourceName}`.*"
      val missRecordsSql = if (!context.runTimeTableRegister.existsTable(targetName)) {
        warn(s"[${timestamp}] data source ${targetName} not exists")
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
        s"SELECT ${selClause} FROM `${sourceName}` " +
          s"LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
      }
      val missRecordsTransStep =
        SparkSqlTransformStep(missRecordsTableName, missRecordsSql, emptyMap, true)

      val missRecordsWriteSteps = procType match {
        case BatchProcessType =>
          val rwName =
            ruleParam.getOutputOpt(RecordOutputType).
              flatMap(_.getNameOpt).getOrElse(missRecordsTableName)
          RecordWriteStep(rwName, missRecordsTableName) :: Nil
        case StreamingProcessType => Nil
      }
      val missRecordsUpdateWriteSteps = procType match {
        case BatchProcessType => Nil
        case StreamingProcessType =>
          val dsName =
            ruleParam.getOutputOpt(DscUpdateOutputType).flatMap(_.getNameOpt).getOrElse(sourceName)
          DataSourceUpdateWriteStep(dsName, missRecordsTableName) :: Nil
      }

      // 2. miss count
      val missCountTableName = "__missCount"
      val missColName = details.getStringOrKey(_miss)
      val missCountSql = procType match {
        case BatchProcessType =>
          s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}`"
        case StreamingProcessType =>
          s"SELECT `${ConstantColumns.tmst}`,COUNT(*) AS `${missColName}` " +
            s"FROM `${missRecordsTableName}` GROUP BY `${ConstantColumns.tmst}`"
      }
      val missCountTransStep = SparkSqlTransformStep(missCountTableName, missCountSql, emptyMap)

      // 3. total count
      val totalCountTableName = "__totalCount"
      val totalColName = details.getStringOrKey(_total)
      val totalCountSql = procType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
        case StreamingProcessType =>
          s"SELECT `${ConstantColumns.tmst}`, COUNT(*) AS `${totalColName}` " +
            s"FROM `${sourceName}` GROUP BY `${ConstantColumns.tmst}`"
      }
      val totalCountTransStep = SparkSqlTransformStep(totalCountTableName, totalCountSql, emptyMap)

      // 4. accuracy metric
      val accuracyTableName = ruleParam.getOutDfName()
      val matchedColName = details.getStringOrKey(_matched)
      val matchedFractionColName = details.getStringOrKey(_matchedFraction)
      val accuracyMetricSql = procType match {
        case BatchProcessType =>
          s"""
             SELECT A.total AS `${totalColName}`,
                    A.miss AS `${missColName}`,
                    (A.total - A.miss) AS `${matchedColName}`,
                    coalesce( (A.total - A.miss) / A.total, 1.0) AS `${matchedFractionColName}`
             FROM (
               SELECT `${totalCountTableName}`.`${totalColName}` AS total,
                      coalesce(`${missCountTableName}`.`${missColName}`, 0) AS miss
               FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
             ) AS A
         """
        case StreamingProcessType =>
          s"""
             |SELECT `${totalCountTableName}`.`${ConstantColumns.tmst}` AS `${ConstantColumns.tmst}`,
             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
             |(`${totalCountTableName}`.`${totalColName}` - coalesce(`${missCountTableName}`.`${missColName}`, 0)) AS `${matchedColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
             |ON `${totalCountTableName}`.`${ConstantColumns.tmst}` = `${missCountTableName}`.`${ConstantColumns.tmst}`
         """.stripMargin
      }
      val accuracyTransStep = SparkSqlTransformStep(accuracyTableName, accuracyMetricSql, emptyMap)
      val accuracyMetricWriteSteps = procType match {
        case BatchProcessType =>
          val metricOpt = ruleParam.getOutputOpt(MetricOutputType)
          val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse(ruleParam.getOutDfName())
          val flattenType = metricOpt.map(_.getFlatten).getOrElse(FlattenType.default)
          MetricWriteStep(mwName, accuracyTableName, flattenType) :: Nil
        case StreamingProcessType => Nil
      }

      // accuracy current steps
      val transSteps1 = missRecordsTransStep :: missCountTransStep :: totalCountTransStep :: accuracyTransStep :: Nil
      val writeSteps1 =
        accuracyMetricWriteSteps ++ missRecordsWriteSteps ++ missRecordsUpdateWriteSteps

      // streaming extra steps
      val (transSteps2, writeSteps2) = procType match {
        case BatchProcessType => (Nil, Nil)
        case StreamingProcessType =>
          // 5. accuracy metric merge
          val accuracyMetricTableName = "__accuracy"
          val accuracyMetricRule = DataFrameOps._accuracy
          val accuracyMetricDetails = Map[String, Any](
            (AccuracyOprKeys._miss -> missColName),
            (AccuracyOprKeys._total -> totalColName),
            (AccuracyOprKeys._matched -> matchedColName)
          )
          val accuracyMetricTransStep = DataFrameOpsTransformStep(accuracyMetricTableName,
            accuracyTableName, accuracyMetricRule, accuracyMetricDetails)
          val accuracyMetricWriteStep = {
            val metricOpt = ruleParam.getOutputOpt(MetricOutputType)
            val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse(ruleParam.getOutDfName())
            val flattenType = metricOpt.map(_.getFlatten).getOrElse(FlattenType.default)
            MetricWriteStep(mwName, accuracyMetricTableName, flattenType)
          }

          // 6. collect accuracy records
          val accuracyRecordTableName = "__accuracyRecords"
          val accuracyRecordSql = {
            s"""
               |SELECT `${ConstantColumns.tmst}`, `${ConstantColumns.empty}`
               |FROM `${accuracyMetricTableName}` WHERE `${ConstantColumns.record}`
             """.stripMargin
          }
          val accuracyRecordTransStep = SparkSqlTransformStep(
            accuracyRecordTableName, accuracyRecordSql, emptyMap)
          val accuracyRecordWriteStep = {
            val rwName =
              ruleParam.getOutputOpt(RecordOutputType).flatMap(_.getNameOpt)
                .getOrElse(missRecordsTableName)

            RecordWriteStep(rwName, missRecordsTableName, Some(accuracyRecordTableName))
          }

          // extra steps
          (accuracyMetricTransStep :: accuracyRecordTransStep :: Nil,
            accuracyMetricWriteStep :: accuracyRecordWriteStep :: Nil)
      }

      // full steps
      transSteps1 ++ transSteps2 ++ writeSteps1 ++ writeSteps2
    }
  }

}
