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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure
import org.apache.griffin.measure.execution.Measure._
import org.apache.griffin.measure.step.builder.ConstantColumns

case class ProfilingMeasure(sparkSession: SparkSession, measureParam: MeasureParam)
    extends Measure {

  import ProfilingMeasure._

  override val supportsRecordWrite: Boolean = false

  override val supportsMetricWrite: Boolean = true

  val roundScale: Int = getFromConfig[java.lang.Integer](RoundScaleStr, 3)
  val approxDistinctCount: Boolean =
    getFromConfig[java.lang.Boolean](ApproxDistinctCountStr, true)

  override def impl(): (DataFrame, DataFrame) = {
    val input = sparkSession.read.table(measureParam.getDataSource)
    val profilingColNames = getFromConfig[String](Expression, input.columns.mkString(","))
      .split(",")
      .map(_.trim.toLowerCase(Locale.ROOT))
      .toSet

    val profilingCols =
      input.schema.fields.filter(f =>
        profilingColNames.contains(f.name) && !f.name.equalsIgnoreCase(ConstantColumns.tmst))

    assert(
      profilingCols.nonEmpty,
      s"Invalid columns [${profilingCols.map(_.name).mkString(", ")}] were provided for profiling.")

    val profilingExprs = profilingCols.foldLeft(Array.empty[Column])((exprList, field) => {
      val colName = field.name
      val profilingExprs = getProfilingExprs(field, roundScale, approxDistinctCount)

      exprList.:+(map(profilingExprs: _*).as(s"$DetailsPrefix$colName"))
    })

    val aggregateDf = profilingCols
      .foldLeft(input)((df, field) => {
        val colName = field.name
        val column = col(colName)

        val lengthColName = lengthColFn(colName)
        val nullColName = nullsInColFn(colName)

        df.withColumn(lengthColName, length(column))
          .withColumn(nullColName, when(isnull(column), 1L).otherwise(0L))
      })
      .agg(count(lit(1L)).as(Total), profilingExprs: _*)

    val detailCols =
      aggregateDf.columns
        .filter(_.startsWith(DetailsPrefix))
        .flatMap(c => Seq(lit(c.stripPrefix(DetailsPrefix)), col(c)))

    val metricDf = aggregateDf
      .withColumn(ColumnDetails, map(detailCols: _*))
      .select(Total, ColumnDetails)
      .select(map(lit(ColumnDetails), col(ColumnDetails)).as(valueColumn))

    (sparkSession.emptyDataFrame, metricDf)
  }

}

object ProfilingMeasure {

  /**
   * Options Keys
   */
  final val RoundScaleStr: String = "round.scale"
  final val ApproxDistinctCountStr: String = "approx.distinct.count"

  /**
   * Structure Keys
   */
  final val ColumnDetails: String = "column_details"
  private final val DataTypeStr: String = "data_type"

  /**
   * Prefix Keys
   */
  private final val ApproxPrefix: String = "approx_"
  private final val DetailsPrefix: String = "details_"
  private final val ColumnLengthPrefix: String = "col_len"
  private final val IsNullPrefix: String = "is_null"

  /**
   * Column Detail Keys
   */
  private final val NullCount: String = "null_count"
  private final val DistinctCount: String = "distinct_count"
  private final val Min: String = "min"
  private final val Max: String = "max"
  private final val Avg: String = "avg"
  private final val StdDeviation: String = "std_dev"
  private final val Variance: String = "variance"
  private final val Kurtosis: String = "kurtosis"
  private final val MinColLength: String = s"${Min}_$ColumnLengthPrefix"
  private final val MaxColLength: String = s"${Max}_$ColumnLengthPrefix"
  private final val AvgColLength: String = s"${Avg}_$ColumnLengthPrefix"

  private def lengthColFn(colName: String): String = s"${ColumnLengthPrefix}_$colName"

  private def nullsInColFn(colName: String): String = s"${IsNullPrefix}_$colName"

  private def forNumericFn(t: DataType, value: Column, alias: String): Column = {
    (if (t.isInstanceOf[NumericType]) value else lit(null)).as(alias)
  }

  private def getProfilingExprs(
      field: StructField,
      roundScale: Int,
      approxDistinctCount: Boolean): Seq[Column] = {
    val colName = field.name
    val colType = field.dataType

    val column = col(colName)
    val lengthColExpr = col(lengthColFn(colName))
    val nullColExpr = col(nullsInColFn(colName))
    val (distinctCountName, distinctCountExpr) =
      if (approxDistinctCount) {
        (
          lit(s"$ApproxPrefix$DistinctCount"),
          approx_count_distinct(column).as(s"$ApproxPrefix$DistinctCount"))
      } else {
        (lit(DistinctCount), countDistinct(column).as(DistinctCount))
      }

    Seq(
      Seq(lit(DataTypeStr), lit(colType.catalogString).as(DataTypeStr)),
      Seq(lit(Total), sum(lit(1)).as(Total)),
      Seq(lit(MinColLength), min(lengthColExpr).as(MinColLength)),
      Seq(lit(MaxColLength), max(lengthColExpr).as(MaxColLength)),
      Seq(lit(AvgColLength), forNumericFn(colType, avg(lengthColExpr), AvgColLength)),
      Seq(lit(Min), forNumericFn(colType, min(column), Min)),
      Seq(lit(Max), forNumericFn(colType, max(column), Max)),
      Seq(lit(Avg), forNumericFn(colType, bround(avg(column), roundScale), Avg)),
      Seq(
        lit(StdDeviation),
        forNumericFn(colType, bround(stddev(column), roundScale), StdDeviation)),
      Seq(lit(Variance), forNumericFn(colType, bround(variance(column), roundScale), Variance)),
      Seq(lit(Kurtosis), forNumericFn(colType, bround(kurtosis(column), roundScale), Kurtosis)),
      Seq(lit(distinctCountName), distinctCountExpr),
      Seq(lit(NullCount), sum(nullColExpr).as(NullCount))).flatten
  }
}
