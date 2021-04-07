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
import org.apache.griffin.measure.step.builder.ConstantColumns

case class AccuracyMeasure(measureParam: MeasureParam) extends Measure {

  case class AccuracyExpr(sourceCol: String, targetCol: String)

  import AccuracyMeasure._
  import Measure._

  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  val targetSource: String = getFromConfig[String](TargetSourceStr, null)
  val exprOpt: Option[Seq[Map[String, String]]] =
    Option(getFromConfig[Seq[Map[String, String]]](Expression, null))

  validate()

  override def impl(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val originalSource = sparkSession.read.table(measureParam.getDataSource)
    val originalCols = originalSource.columns

    val dataSource = addColumnPrefix(originalSource, SourcePrefixStr)

    val targetDataSource =
      addColumnPrefix(
        sparkSession.read.table(targetSource).drop(ConstantColumns.tmst),
        TargetPrefixStr)

    val accuracyExprs = exprOpt.get
      .map(toAccuracyExpr)
      .distinct
      .map(x =>
        AccuracyExpr(s"$SourcePrefixStr${x.sourceCol}", s"$TargetPrefixStr${x.targetCol}"))

    val joinExpr =
      accuracyExprs
        .map(e => col(e.sourceCol) === col(e.targetCol))
        .reduce(_ and _)

    val indicatorExpr =
      accuracyExprs
        .map(e =>
          coalesce(col(e.sourceCol), emptyCol) notEqual coalesce(col(e.targetCol), emptyCol))
        .reduce(_ or _)

    val nullExpr = accuracyExprs.map(e => col(e.sourceCol).isNull).reduce(_ or _)

    val recordsDf = removeColumnPrefix(
      dataSource
        .join(targetDataSource, joinExpr, "left")
        .withColumn(valueColumn, when(indicatorExpr or nullExpr, 1).otherwise(0)),
      SourcePrefixStr)
      .select((originalCols :+ valueColumn).map(col): _*)

    val selectCols =
      Seq(Total, AccurateStr, InAccurateStr).flatMap(e => Seq(lit(e), col(e).cast("string")))
    val metricColumn: Column = map(selectCols: _*).as(valueColumn)

    val metricDf = recordsDf
      .withColumn(Total, lit(1))
      .agg(sum(Total).as(Total), sum(valueColumn).as(InAccurateStr))
      .withColumn(AccurateStr, col(Total) - col(InAccurateStr))
      .select(metricColumn)

    (recordsDf, metricDf)
  }

  private def validate(): Unit = {
    assert(exprOpt.isDefined, s"'$Expression' must be defined.")
    assert(exprOpt.get.flatten.nonEmpty, s"'$Expression' must not be empty or of invalid type.")

    assert(
      !StringUtil.isNullOrEmpty(targetSource),
      s"'$TargetSourceStr' must not be null, empty or of invalid type.")

    datasetValidations()
  }

  private def toAccuracyExpr(map: Map[String, String]): AccuracyExpr = {
    assert(map.contains(SourceColStr), s"'$SourceColStr' must be defined.")
    assert(map.contains(TargetColStr), s"'$TargetColStr' must be defined.")

    AccuracyExpr(map(SourceColStr), map(TargetColStr))
  }

  private def datasetValidations(): Unit = {
    val sparkSession = SparkSession.getDefaultSession.get

    assert(
      sparkSession.catalog.tableExists(targetSource),
      s"Target source with name '$targetSource' does not exist.")

    val datasourceName = measureParam.getDataSource

    val dataSourceCols =
      sparkSession.read.table(datasourceName).columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val targetDataSourceCols =
      sparkSession.read.table(targetSource).columns.map(_.toLowerCase(Locale.ROOT)).toSet

    val accuracyExpr = exprOpt.get.map(toAccuracyExpr).distinct
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

  private def addColumnPrefix(dataFrame: DataFrame, prefix: String): DataFrame = {
    val columns = dataFrame.columns
    columns.foldLeft(dataFrame)((df, c) => df.withColumnRenamed(c, s"$prefix$c"))
  }

  private def removeColumnPrefix(dataFrame: DataFrame, prefix: String): DataFrame = {
    val columns = dataFrame.columns
    columns.foldLeft(dataFrame)((df, c) => df.withColumnRenamed(c, c.stripPrefix(prefix)))
  }
}

object AccuracyMeasure {
  final val SourcePrefixStr: String = "__source_"
  final val TargetPrefixStr: String = "__target_"

  final val TargetSourceStr: String = "target.source"
  final val SourceColStr: String = "source.col"
  final val TargetColStr: String = "target.col"

  final val AccurateStr: String = "accurate"
  final val InAccurateStr: String = "inaccurate"
}
