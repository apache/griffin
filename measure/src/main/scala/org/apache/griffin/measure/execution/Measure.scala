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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.griffin.measure.Loggable

trait Measure extends Loggable {
  import Measure._

  val measureParam: MeasureParam

  val supportsRecordWrite: Boolean
  val supportsMetricWrite: Boolean

  final val measureViolationsColName = s"${MeasureColPrefix}_${measureParam.getName}"

  def getFromConfig[T: ClassTag](key: String, defValue: T): T = {
    measureParam.getConfig.getAnyRef[T](key, defValue)
  }

  def preProcessMetrics(input: DataFrame): DataFrame = {
    val measureType = measureParam.getType.toString.toLowerCase(Locale.ROOT)

    input
      .withColumn(MeasureName, typedLit[String](measureParam.getName))
      .withColumn(MeasureType, typedLit[String](measureType))
      .withColumn("value", col(measureViolationsColName))
      .withColumn("data_source", typedLit[String](measureParam.getDataSource))
      .select(MeasureName, MeasureType, "data_source", "value")
  }

  def preProcessRecords(input: DataFrame): DataFrame = {
    input
      .withColumn(Status, when(col(measureViolationsColName) === 0, "good").otherwise("bad"))
      .drop(measureViolationsColName)
  }

  def impl(sparkSession: SparkSession): (DataFrame, DataFrame)

  def execute(sparkSession: SparkSession, batchId: Option[Long]): (DataFrame, DataFrame) = {
    val (badRecordsDf, metricDf) = impl(sparkSession)

    var batchDetailsOpt = StringUtils.EMPTY
    val res = batchId match {
      case Some(batchId) =>
        implicit val bId: Long = batchId
        batchDetailsOpt = s"for batch id $bId"
        (appendBatchIdIfAvailable(badRecordsDf), appendBatchIdIfAvailable(metricDf))
      case None => (badRecordsDf, metricDf)
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
  final val Expression = "expr"
  final val MeasureColPrefix = "__measure"
  final val Status = "__status"
  final val BatchId = "__batch_id"
  final val MeasureName = "measure_name"
  final val MeasureType = "measure_type"
}
