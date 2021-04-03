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

package org.apache.griffin.measure.execution

import java.util.Locale

import scala.reflect.ClassTag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.griffin.measure.Loggable

trait Measure extends Loggable {
  import Measure._

  val measureParam: MeasureParam

  val supportsRecordWrite: Boolean
  val supportsMetricWrite: Boolean

  final val valueColumn = s"${MeasureColPrefix}_${measureParam.getName}"

  def getFromConfig[T: ClassTag](key: String, defValue: T): T = {
    measureParam.getConfig.getAnyRef[T](key, defValue)
  }

  // todo add status col to persist blank metrics if the measure fails
  def preProcessMetrics(input: DataFrame): DataFrame = {
    if (supportsMetricWrite) {
      val measureType = measureParam.getType.toString.toLowerCase(Locale.ROOT)

      input
        .withColumn(MeasureName, typedLit[String](measureParam.getName))
        .withColumn(MeasureType, typedLit[String](measureType))
        .withColumn(Metrics, col(valueColumn))
        .withColumn(DataSource, typedLit[String](measureParam.getDataSource))
        .select(MeasureName, MeasureType, DataSource, Metrics)
    } else input
  }

  def preProcessRecords(input: DataFrame): DataFrame = {
    if (supportsRecordWrite) {
      input
        .withColumn(Status, when(col(valueColumn) === 0, Good).otherwise(Bad))
        .drop(valueColumn)
    } else input
  }

  def impl(sparkSession: SparkSession): (DataFrame, DataFrame)

  def execute(sparkSession: SparkSession, batchId: Option[Long]): (DataFrame, DataFrame) = {
    val (recordsDf, metricDf) = impl(sparkSession)

    val processedRecordDf = preProcessRecords(recordsDf)
    val processedMetricDf = preProcessMetrics(metricDf)

    var batchDetailsOpt = StringUtils.EMPTY
    val res = batchId match {
      case Some(batchId) =>
        implicit val bId: Long = batchId
        batchDetailsOpt = s"for batch id $bId"
        (appendBatchIdIfAvailable(processedRecordDf), appendBatchIdIfAvailable(processedMetricDf))
      case None => (processedRecordDf, processedMetricDf)
    }

    info(
      s"Execution of '${measureParam.getType}' measure " +
        s"with name '${measureParam.getName}' is complete $batchDetailsOpt")

    res
  }

  private def appendBatchIdIfAvailable(input: DataFrame)(implicit batchId: Long): DataFrame = {
    input.withColumn(BatchId, typedLit[Long](batchId))
  }

}

object Measure {

  final val DataSource = "data_source"
  final val Expression = "expr"
  final val MeasureColPrefix = "__measure"
  final val Status = "__status"
  final val BatchId = "__batch_id"
  final val MeasureName = "measure_name"
  final val MeasureType = "measure_type"
  final val Metrics = "metrics"
  final val Good = "good"
  final val Bad = "bad"

  final val Total: String = "total"
  final val BadRecordDefinition = "bad.record.definition"
  final val AllColumns: String = "*"

  final val emptyCol: Column = lit(StringUtils.EMPTY)

}
