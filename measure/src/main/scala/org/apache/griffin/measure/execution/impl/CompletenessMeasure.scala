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

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure

case class CompletenessMeasure(measureParam: MeasureParam) extends Measure {

  import Measure._

  private final val Complete: String = "complete"
  private final val InComplete: String = "incomplete"

  override val supportsRecordWrite: Boolean = true

  override val supportsMetricWrite: Boolean = true

  override def impl(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val exprOpt = Option(getFromConfig[String](Expression, null))

    val column = exprOpt match {
      case Some(exprStr) => when(expr(exprStr), 1.0).otherwise(0.0)
      case None =>
        error(
          s"$Expression was not defined for completeness measure.",
          new IllegalArgumentException(s"$Expression was not defined for completeness measure."))
        throw new IllegalArgumentException(
          s"$Expression was not defined for completeness measures")
    }

    val selectCols = Seq(Total, Complete, InComplete).flatMap(e => Seq(lit(e), col(e)))
    val metricColumn: Column = map(selectCols: _*).as(valueColumn)

    val input = sparkSession.read.table(measureParam.getDataSource)
    val badRecordsDf = input.withColumn(valueColumn, column)

    val metricDf = badRecordsDf
      .withColumn(Total, lit(1))
      .agg(sum(Total).as(Total), sum(valueColumn).as(InComplete))
      .withColumn(Complete, col(Total) - col(InComplete))
      .select(metricColumn)

    (badRecordsDf, metricDf)
  }
}
