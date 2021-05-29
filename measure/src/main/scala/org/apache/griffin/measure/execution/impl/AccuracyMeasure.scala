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

case class AccuracyMeasure(sparkSession: SparkSession, measureParam: MeasureParam)
    extends Measure {

  case class AccuracyExpr(sourceCol: String, refCol: String)

  import AccuracyMeasure._
  import Measure._

  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  val refSource: String = getFromConfig[String](ReferenceSourceStr, null)
  val exprOpt: Option[Seq[Map[String, String]]] =
    Option(getFromConfig[Seq[Map[String, String]]](Expression, null))

  validate()

  override def impl(): (DataFrame, DataFrame) = {
    val originalSource = sparkSession.read.table(measureParam.getDataSource)
    val originalCols = originalSource.columns

    val dataSource = addColumnPrefix(originalSource, SourcePrefixStr)

    val refDataSource =
      addColumnPrefix(sparkSession.read.table(refSource).drop(ConstantColumns.tmst), refPrefixStr)

    val accuracyExprs = exprOpt.get
      .map(toAccuracyExpr)
      .distinct
      .map(x => AccuracyExpr(s"$SourcePrefixStr${x.sourceCol}", s"$refPrefixStr${x.refCol}"))

    val joinExpr =
      accuracyExprs
        .map(e => col(e.sourceCol) === col(e.refCol))
        .reduce(_ and _)

    val indicatorExpr =
      accuracyExprs
        .map(e => coalesce(col(e.sourceCol), emptyCol) notEqual coalesce(col(e.refCol), emptyCol))
        .reduce(_ or _)

    val nullExpr = accuracyExprs.map(e => col(e.sourceCol).isNull).reduce(_ or _)

    val recordsDf = removeColumnPrefix(
      dataSource
        .join(refDataSource, joinExpr, "left")
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

  override def validate(): Unit = {
    assert(exprOpt.isDefined, s"'$Expression' must be defined.")
    assert(exprOpt.get.flatten.nonEmpty, s"'$Expression' must not be empty or of invalid type.")

    assert(
      !StringUtil.isNullOrEmpty(refSource),
      s"'$ReferenceSourceStr' must not be null, empty or of invalid type.")

    datasetValidations()
  }

  private def toAccuracyExpr(map: Map[String, String]): AccuracyExpr = {
    assert(map.contains(SourceColStr), s"'$SourceColStr' must be defined.")
    assert(map.contains(ReferenceColStr), s"'$ReferenceColStr' must be defined.")

    AccuracyExpr(map(SourceColStr), map(ReferenceColStr))
  }

  private def datasetValidations(): Unit = {
    assert(
      sparkSession.catalog.tableExists(refSource),
      s"Reference source with name '$refSource' does not exist.")

    val datasourceName = measureParam.getDataSource

    val dataSourceCols =
      sparkSession.read.table(datasourceName).columns.map(_.toLowerCase(Locale.ROOT)).toSet
    val refDataSourceCols =
      sparkSession.read.table(refSource).columns.map(_.toLowerCase(Locale.ROOT)).toSet

    val accuracyExpr = exprOpt.get.map(toAccuracyExpr).distinct
    val (forDataSource, forRefDataSource) =
      accuracyExpr
        .map(
          e =>
            (
              (e.sourceCol, dataSourceCols.contains(e.sourceCol)),
              (e.refCol, refDataSourceCols.contains(e.refCol))))
        .unzip

    val invalidColsDataSource = forDataSource.filterNot(_._2)
    val invalidColsRefSource = forRefDataSource.filterNot(_._2)

    assert(
      invalidColsDataSource.isEmpty,
      s"Column(s) [${invalidColsDataSource.map(_._1).mkString(", ")}] " +
        s"do not exist in data set with name '$datasourceName'")

    assert(
      invalidColsRefSource.isEmpty,
      s"Column(s) [${invalidColsRefSource.map(_._1).mkString(", ")}] " +
        s"do not exist in reference data set with name '$refSource'")
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
  final val refPrefixStr: String = "__ref_"

  final val ReferenceSourceStr: String = "ref.source"
  final val SourceColStr: String = "source.col"
  final val ReferenceColStr: String = "ref.col"

  final val AccurateStr: String = "accurate"
  final val InAccurateStr: String = "inaccurate"
}
