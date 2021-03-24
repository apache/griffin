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

import org.apache.spark.sql.DataFrame

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.enums.{MeasureTypes, OutputType}
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.execution.impl._
import org.apache.griffin.measure.step.write.{MetricFlushStep, MetricWriteStep, RecordWriteStep}

case class MeasureExecutor(context: DQContext) extends Loggable {

  def execute(measureParams: Seq[MeasureParam]): Unit = {
    cacheIfNecessary(measureParams)

    measureParams
      .groupBy(measureParam => measureParam.getDataSource)
      .foreach(measuresForSource => {
        val dataSourceName = measuresForSource._1
        val measureParams = measuresForSource._2

        val dataSource = context.sparkSession.read.table(dataSourceName)

        if (dataSource.isStreaming) {
          // todo this is a no op as streaming queries need to be registered.
          dataSource.writeStream
            .foreachBatch((_, batchId) => {
              executeMeasures(measureParams, Some(batchId))
            })
        } else {
          executeMeasures(measureParams)
        }
      })
  }

  private def cacheIfNecessary(measureParams: Seq[MeasureParam]): Unit = {
    measureParams
      .map(_.getDataSource)
      .groupBy(x => x)
      .mapValues(_.length)
      .filter(_._2 > 1)
      .foreach(source => {
        info(
          s"Caching data source with name '${source._1}'" +
            s" as ${source._2} measures are applied on it.")
        context.sparkSession.catalog.cacheTable(source._1)
      })
  }

  private def executeMeasures(
      measureParams: Seq[MeasureParam],
      batchId: Option[Long] = None): Unit = {
    measureParams.foreach(measureParam => {
      val measure = createMeasure(measureParam)
      val (badRecordsDf, metricsDf) = measure.execute(context.sparkSession, batchId)

      persistRecords(measure, badRecordsDf)
      persistMetrics(measure, metricsDf)
    })
  }

  private def createMeasure(measureParam: MeasureParam): Measure = {
    measureParam.getType match {
      case MeasureTypes.Completeness => CompletenessMeasure(measureParam)
      case _ =>
        val errorMsg = s"Measure type '${measureParam.getType}' is not supported."
        val exception = new NotImplementedError(errorMsg)
        error(errorMsg, exception)
        throw exception
    }
  }

  private def persistRecords(measure: Measure, recordsDf: DataFrame): Unit = {
    val measureParam: MeasureParam = measure.measureParam

    measureParam.getOutputOpt(OutputType.RecordOutputType) match {
      case Some(_) =>
        if (measure.supportsRecordWrite) {
          recordsDf.createOrReplaceTempView("badRecordsDf")
          RecordWriteStep(measureParam.getName, "badRecordsDf").execute(context)
        } else warn(s"Measure with name '${measureParam.getName}' doesn't support record write")
      case None =>
    }
  }

  private def persistMetrics(measure: Measure, metricsDf: DataFrame): Unit = {
    val measureParam: MeasureParam = measure.measureParam

    measureParam.getOutputOpt(OutputType.MetricOutputType) match {
      case Some(o) =>
        if (measure.supportsMetricWrite) {
          metricsDf.createOrReplaceTempView("metricsDf")
          MetricWriteStep(measureParam.getName, "metricsDf", o.getFlatten)
            .execute(context)
          MetricFlushStep().execute(context)
        } else warn(s"Measure with name '${measureParam.getName}' doesn't support metric write")
      case None =>
    }
  }

}
