package org.apache.griffin.measure.execution.impl

import io.netty.util.internal.StringUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr => sparkExpr, _}
import org.apache.spark.sql.types.BooleanType

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure

case class SparkSQLMeasure(measureParam: MeasureParam) extends Measure {

  import Measure._

  final val Complete: String = "complete"
  final val InComplete: String = "incomplete"

  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  private val expr = getFromConfig[String](Expression, StringUtils.EMPTY)
  private val badnessExpr = getFromConfig[String](BadRecordDefinition, StringUtils.EMPTY)

  validate()

  override def impl(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val df = sparkSession.sql(expr).withColumn(valueColumn, sparkExpr(badnessExpr))

    assert(
      df.schema.exists(f => f.name.matches(valueColumn) && f.dataType.isInstanceOf[BooleanType]),
      s"Invalid condition provided as $BadRecordDefinition. Does not yield a boolean result.")

    val selectCols =
      Seq(Total, Complete, InComplete).flatMap(e => Seq(lit(e), col(e).cast("string")))
    val metricColumn: Column = map(selectCols: _*).as(valueColumn)

    val badRecordsDf = df.withColumn(valueColumn, when(col(valueColumn), 1).otherwise(0))
    val metricDf = badRecordsDf
      .withColumn(Total, lit(1))
      .agg(sum(Total).as(Total), sum(valueColumn).as(InComplete))
      .withColumn(Complete, col(Total) - col(InComplete))
      .select(metricColumn)

    (badRecordsDf, metricDf)
  }

  private def validate(): Unit = {
    assert(
      !StringUtil.isNullOrEmpty(expr),
      "Invalid query provided as expr. Must not be null, empty or of invalid type.")
    assert(
      !StringUtil.isNullOrEmpty(badnessExpr),
      "Invalid condition provided as bad.record.definition.")
  }
}
