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

package org.apache.griffin.measure.execution.impl

import java.util.Locale

import io.netty.util.internal.StringUtil
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure

case class AccuracyMeasure(measureParam: MeasureParam) extends Measure {

  case class AccuracyExpr(sourceCol: String, targetCol: String)

  import Measure._

  private final val TargetSourceStr: String = "target.source"
  private final val SourceColStr: String = "source.col"
  private final val TargetColStr: String = "target.col"

  private final val AccurateStr: String = "accurate"
  private final val InAccurateStr: String = "inaccurate"

  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  val targetSource: String = getFromConfig[String](TargetSourceStr, null)
  val exprOpt: Option[Seq[AccuracyExpr]] =
    Option(getFromConfig[Seq[Map[String, String]]](Expression, null).map(toAccuracyExpr).distinct)

  validate()

  override def impl(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    import org.apache.griffin.measure.step.builder.ConstantColumns
    datasetValidations(sparkSession)

    val dataSource = sparkSession.read.table(measureParam.getDataSource)
    val targetDataSource = sparkSession.read.table(targetSource).drop(ConstantColumns.tmst)

    exprOpt match {
      case Some(accuracyExpr) =>
        val joinExpr =
          accuracyExpr.map(e => col(e.sourceCol) === col(e.targetCol)).reduce(_ and _)

        val indicatorExpr =
          accuracyExpr
            .map(e =>
              coalesce(col(e.sourceCol), emptyCol) notEqual coalesce(col(e.targetCol), emptyCol))
            .reduce(_ or _)

        val recordsDf = targetDataSource
          .join(dataSource, joinExpr, "outer")
          .withColumn(valueColumn, when(indicatorExpr, 1).otherwise(0))
          .selectExpr(s"${measureParam.getDataSource}.*", valueColumn)

        val selectCols = Seq(Total, AccurateStr, InAccurateStr).flatMap(e => Seq(lit(e), col(e)))
        val metricColumn: Column = map(selectCols: _*).as(valueColumn)

        val metricDf = recordsDf
          .withColumn(Total, lit(1))
          .agg(sum(Total).as(Total), sum(valueColumn).as(InAccurateStr))
          .withColumn(AccurateStr, col(Total) - col(InAccurateStr))
          .select(metricColumn)

        (recordsDf, metricDf)
      case None =>
        throw new IllegalArgumentException(s"'$Expression' must be defined.")
    }
  }

  private def validate(): Unit = {
    assert(exprOpt.isDefined, s"'$Expression' must be defined.")
    assert(exprOpt.nonEmpty, s"'$Expression' must not be empty.")

    assert(
      !StringUtil.isNullOrEmpty(targetSource),
      s"'$TargetSourceStr' must not be null or empty.")
  }

  private def toAccuracyExpr(map: Map[String, String]): AccuracyExpr = {
    assert(map.contains(SourceColStr), s"'$SourceColStr' must be defined.")
    assert(map.contains(TargetColStr), s"'$TargetColStr' must be defined.")

    AccuracyExpr(map(SourceColStr), map(TargetColStr))
  }

  private def datasetValidations(sparkSession: SparkSession): Unit = {
    assert(
      sparkSession.catalog.tableExists(targetSource),
      s"Target source with name '$targetSource' does not exist.")

    val datasourceName = measureParam.getDataSource

    val dataSourceCols =
      sparkSession.read.table(datasourceName).columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val targetDataSourceCols =
      sparkSession.read.table(targetSource).columns.map(_.toLowerCase(Locale.ROOT)).toSet

    val accuracyExpr = exprOpt.get
    val (forDataSource, forTarget) =
      accuracyExpr
        .map(
          e =>
            (
              (e.sourceCol, dataSourceCols.contains(e.sourceCol)),
              (e.targetCol, targetDataSourceCols.contains(e.targetCol))))
        .unzip

    val invalidColsDataSource = forDataSource.filterNot(_._2)
    val invalidColsTarget = forTarget.filterNot(_._2)

    assert(
      invalidColsDataSource.isEmpty,
      s"Column(s) [${invalidColsDataSource.map(_._1).mkString(", ")}] " +
        s"do not exist in data set with name '$datasourceName'")

    assert(
      invalidColsTarget.isEmpty,
      s"Column(s) [${invalidColsTarget.map(_._1).mkString(", ")}] " +
        s"do not exist in target data set with name '$targetSource'")
  }
}
