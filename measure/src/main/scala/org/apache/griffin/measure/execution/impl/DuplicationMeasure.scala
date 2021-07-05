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
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure

/**
 * Duplication Measure.
 *
 * Asserting the measure of duplication of the entities within a data set implies that
 * no entity exists more than once within the data set and that there is a key that can be used
 * to uniquely access each entity.
 *
 * For example, in a master product table, each product must appear once and be assigned a unique
 * identifier that represents that product within a system or across multiple applications/ systems.
 *
 * Duplication measures the redundancies in a dataset in terms of the following metrics,
 *  - Duplicate: the number of values that are the same as other values in the list
 *  - Distinct: the number of non-null values that are different from each other (Non-unique + Unique)
 *  - Non Unique: the number of values that have at least one duplicate in the list
 *  - Unique: the number of values that have no duplicates
 *
 * @param sparkSession SparkSession for this Griffin Application.
 * @param measureParam Object representation of this measure and its configuration.
 */
case class DuplicationMeasure(sparkSession: SparkSession, measureParam: MeasureParam)
    extends Measure {

  import DuplicationMeasure._
  import Measure._

  /**
   * Metrics of redundancies
   */
  private final val duplicationMeasures = Seq(Total, Duplicate, Unique, NonUnique, Distinct)

  /**
   * The value for `expr` is a comma separated string of columns in the data asset on which the
   * duplication measure is to be executed. `expr` is an optional key for Duplication measure, i.e.,
   * if it is not defined, the entire row will be checked by duplication measure.
   */
  val exprs: String = getFromConfig[String](Expression, null)

  /**
   * Its value defines what exactly would be considered as a bad record after this measure
   * computes redundancies on the data asset. Since the redundancies are calculated as `duplicate`,
   * `unique`, `non_unique`, and  `distinct`, the value of this key must also be one of these values.
   * This key is mandatory and must be defined with appropriate value.
   */
  private val badnessExpr = getFromConfig[String](BadRecordDefinition, StringUtils.EMPTY)

  validate()

  /**
   * Duplication measure supports record and metric write
   */
  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  /**
   * The Duplication measure calculates the all metrics of redundancies for the input dataset.
   * Users can choose which of these metrics defines a "bad record" for them by defining `BadRecordDefinition`
   * with a supported value.
   *
   * Duplication produces the following 5 metrics as result,
   *  - Total records
   *  - Duplicate records
   *  - Unique records
   *  - NonUnique records
   *  - Distinct records
   *
   *  @return tuple of records dataframe and metric dataframe
   */
  override def impl(): (DataFrame, DataFrame) = {
    val input = sparkSession.read.table(measureParam.getDataSource)
    val cols = keyCols(input).map(col)

    val isNullCol = cols.map(x => x.isNull).reduce(_ and _)
    val duplicateCol = when(col(__Temp) > 1, 1).otherwise(0)
    val uniqueCol = when(not(isNullCol) and col(Unique) === 1, 1).otherwise(0)
    val distinctCol =
      when(not(isNullCol) and (col(Unique) === 1 or col(NonUnique) === 1), 1).otherwise(0)
    val nonUniqueCol =
      when(not(isNullCol) and col(Unique) =!= 1 and (col(__Temp) - col(NonUnique) === 0), 1)
        .otherwise(0)

    val window = Window.partitionBy(cols: _*).orderBy(cols: _*)

    val aggDf = input
      .select(col(AllColumns), row_number().over(window).as(__Temp))
      .withColumn(IsNull, isNullCol)
      .withColumn(Duplicate, duplicateCol)
      .withColumn(Unique, count(lit(1)).over(window))
      .withColumn(Unique, uniqueCol)
      .withColumn(NonUnique, min(__Temp).over(window))
      .withColumn(NonUnique, nonUniqueCol)
      .withColumn(Distinct, distinctCol)
      .withColumn(Total, lit(1))
      .withColumn(valueColumn, col(badnessExpr))
      .drop(__Temp, IsNull)

    val metricAggCols = duplicationMeasures.map(m => sum(m).as(m))

    val selectCols = duplicationMeasures.map(e =>
      map(lit(MetricName), lit(e), lit(MetricValue), col(e).cast(StringType)))
    val metricColumn: Column = array(selectCols: _*).as(valueColumn)

    val metricDf = aggDf
      .agg(metricAggCols.head, metricAggCols.tail: _*)
      .select(metricColumn)

    val badRecordsDf = aggDf.drop(duplicationMeasures: _*)

    (badRecordsDf, metricDf)
  }

  /**
   * Since `expr` is a comma separated string of columns, these provided columns must exist in the dataset.
   * `BadRecordDefinition` must be defined with one of the supported values.
   */
  override def validate(): Unit = {
    val input = sparkSession.read.table(measureParam.getDataSource)
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

/**
 * Duplication measure constants
 */
object DuplicationMeasure {
  final val IsNull: String = "is_null"
  final val Duplicate: String = "duplicate"
  final val Unique: String = "unique"
  final val NonUnique: String = "non_unique"
  final val Distinct: String = "distinct"
  final val __Temp: String = "__temp"
}
