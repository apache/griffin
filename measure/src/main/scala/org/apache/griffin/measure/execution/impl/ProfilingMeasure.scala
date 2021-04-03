package org.apache.griffin.measure.execution.impl

import java.util.Locale

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure
import org.apache.griffin.measure.step.builder.ConstantColumns

case class ProfilingMeasure(measureParam: MeasureParam) extends Measure {

  import Measure._
  import ProfilingMeasure._

  override val supportsRecordWrite: Boolean = false

  override val supportsMetricWrite: Boolean = true

  private val roundScale: Int = getFromConfig[java.lang.Integer](RoundScaleStr, 3)
  private val approxDistinctCount: Boolean =
    getFromConfig[java.lang.Boolean](ApproxDistinctCountStr, true)

  override def impl(sparkSession: SparkSession): (DataFrame, DataFrame) = {
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
      .agg(count(lit(1L)).as(TotalCount), profilingExprs: _*)

    val detailCols =
      aggregateDf.columns
        .filter(_.startsWith(DetailsPrefix))
        .flatMap(c => Seq(lit(c.stripPrefix(DetailsPrefix)), col(c)))

    val metricDf = aggregateDf
      .withColumn(ColumnDetails, map(detailCols: _*))
      .select(TotalCount, ColumnDetails)
      .select(map(lit(ColumnDetails), col(ColumnDetails)).as(valueColumn))

    (sparkSession.emptyDataFrame, metricDf)
  }

}

object ProfilingMeasure {

  /**
   * Options Keys
   */
  private final val RoundScaleStr: String = "round.scale"
  private final val ApproxDistinctCountStr: String = "approx.distinct.count"

  /**
   * Structure Keys
   */
  private final val ColumnDetails: String = "column_details"
  private final val ColName: String = "col_name"
  private final val DataTypeStr: String = "data_type"
  private final val TotalCount: String = "total_count"

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

  private final val AllColumns: String = "*"

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
      if (approxDistinctCount)
        (
          lit(s"$ApproxPrefix$DistinctCount"),
          approx_count_distinct(column).as(s"$ApproxPrefix$DistinctCount"))
      else (lit(DistinctCount), countDistinct(column).as(DistinctCount))

    Seq(
      Seq(lit(DataTypeStr), lit(colType.catalogString).as(DataTypeStr)),
      Seq(lit(TotalCount), sum(lit(1)).as(TotalCount)),
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
