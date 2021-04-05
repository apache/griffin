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

import io.netty.util.internal.StringUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure

/**
 * Duplication Measure
 *
 * Definition of duplication measures used:
 *  - Duplicate: the number of values that are the same as other values in the list
 *  - Distinct: the number of non-null values that are different from each other (Non-unique + Unique)
 *  - Non Unique: the number of values that have at least one duplicate in the list
 *  - Unique: the number of values that have no duplicates
 *
 * @param measureParam Measure Param
 */
case class DuplicationMeasure(measureParam: MeasureParam) extends Measure {

  import DuplicationMeasure._
  import Measure._

  private final val duplicationMeasures = Seq(Duplicate, Unique, NonUnique, Distinct)

  val exprs: String = getFromConfig[String](Expression, null)
  private val badnessExpr = getFromConfig[String](BadRecordDefinition, StringUtils.EMPTY)

  validate()

  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  override def impl(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val input = sparkSession.read.table(measureParam.getDataSource)

    val duplicateCol = when(col(__Temp) > 1, 1).otherwise(0)
    val uniqueCol = when(col(Unique) =!= 1, 0).otherwise(1)
    val distinctCol = when(col(Unique) === 1 or col(NonUnique) === 1, 1).otherwise(0)
    val nonUniqueCol =
      when(col(Unique) =!= 1 and (col(__Temp) - col(NonUnique) === 0), 1).otherwise(0)

    val cols = keyCols(input).map(col)
    val window = Window.partitionBy(cols: _*).orderBy(cols: _*)

    val aggDf = input
      .select(col(AllColumns), row_number().over(window).as(__Temp))
      .withColumn(Duplicate, duplicateCol)
      .withColumn(Unique, count(lit(1)).over(window))
      .withColumn(Unique, uniqueCol)
      .withColumn(NonUnique, min(__Temp).over(window))
      .withColumn(NonUnique, nonUniqueCol)
      .withColumn(Distinct, distinctCol)
      .withColumn(valueColumn, col(badnessExpr))
      .drop(__Temp)

    val metricAggCols = duplicationMeasures.map(m => sum(m).as(m))
    val selectCols = duplicationMeasures.flatMap(e => Seq(lit(e), col(e).cast("string")))
    val metricColumn = map(selectCols: _*).as(valueColumn)
    val metricDf = aggDf
      .agg(metricAggCols.head, metricAggCols.tail: _*)
      .select(metricColumn)

    val badRecordsDf = aggDf.drop(duplicationMeasures: _*)

    (badRecordsDf, metricDf)
  }

  private def validate(): Unit = {
    val input = SparkSession.getDefaultSession.get.read.table(measureParam.getDataSource)
    val kc = keyCols(input)

    assert(kc.nonEmpty, s"Columns defined in '$Expression' is empty.")
    kc.foreach(c =>
      assert(input.columns.contains(c), s"Provided column '$c' does not exist in the dataset."))

    assert(
      !StringUtil.isNullOrEmpty(badnessExpr),
      s"Invalid value '$badnessExpr' provided for $BadRecordDefinition")

    assert(badnessExpr match {
      case Duplicate | Unique | NonUnique | Distinct => true
      case _ => false
    }, s"Invalid value '$badnessExpr' was provided for $BadRecordDefinition")
  }

  private def keyCols(input: DataFrame): Array[String] = {
    if (StringUtil.isNullOrEmpty(exprs)) input.columns
    else exprs.split(",").map(_.trim)
  }.distinct

}

object DuplicationMeasure {
  final val Duplicate: String = "duplicate"
  final val Unique: String = "unique"
  final val NonUnique: String = "non_unique"
  final val Distinct: String = "distinct"
  final val __Temp: String = "__temp"
}
