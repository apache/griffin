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

import java.util.Date
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.configuration.enums.{MeasureTypes, OutputType}
import org.apache.griffin.measure.configuration.enums.FlattenType.DefaultFlattenType
import org.apache.griffin.measure.context.{ContextId, DQContext}
import org.apache.griffin.measure.execution.impl._
import org.apache.griffin.measure.step.write.{MetricFlushStep, MetricWriteStep, RecordWriteStep}

/**
 * MeasureExecutor
 *
 * This acts as the starting point for the execution of different data quality measures
 * defined by the users in `DQConfig`. Execution of the measures involves the following steps,
 *  - Create a fix pool of threads which will be used to execute measures in parallel
 *  - For each measure defined per data source,
 *    - Caching data source(s) if necessary
 *    - In parallel do the following,
 *      - Create Measure entity (transformation step)
 *      - Write Metrics if required and if supported (metric write step)
 *      - Write Records if required and if supported (record write step)
 *      - Clear internal objects (metric flush step)
 *    - Un caching data source(s) if cached already.
 *
 * In contrast to the execution of `GriffinDslDQStepBuilder`, `MeasureExecutor` executes each of
 * the defined measures independently. This means that the outputs (metrics and records) are written
 * independently for each measure.
 *
 * @param context Instance of `DQContext`
 */
case class MeasureExecutor(context: DQContext) extends Loggable {

  /**
   * SparkSession for this Griffin Application.
   */
  private val sparkSession: SparkSession = context.sparkSession

  /**
   * Enable or disable caching of data sources before execution. Defaults to `true`.
   */
  private val cacheDataSources: Boolean = sparkSession.sparkContext.getConf
    .getBoolean("spark.griffin.measure.cacheDataSources", defaultValue = true)

  /**
   * Size of thread pool for parallel measure execution.
   * Defaults to number of processors available to the spark driver JVM.
   */
  private val numThreads: Int = sparkSession.sparkContext.getConf
    .getInt("spark.griffin.measure.parallelism", Runtime.getRuntime.availableProcessors())

  /**
   * Service to handle threaded execution of tasks (measures).
   */
  private implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numThreads))

  /**
   * Starting point of measure execution.
   *
   * @param measureParams Object representation(s) of user defined measure(s).
   */
  def execute(measureParams: Seq[MeasureParam]): Unit = {
    implicit val measureCountByDataSource: Map[String, Int] = measureParams
      .map(_.getDataSource)
      .groupBy(x => x)
      .mapValues(_.length)

    measureParams
      .groupBy(measureParam => measureParam.getDataSource)
      .foreach(measuresForSource => {
        val dataSourceName = measuresForSource._1
        val measureParams = measuresForSource._2

        withCacheIfNecessary(dataSourceName, {
          val dataSource = sparkSession.read.table(dataSourceName)

          if (dataSource.isStreaming) {
            // TODO this is a no op as streaming queries need to be registered.

            dataSource.writeStream
              .foreachBatch((_, batchId) => {
                executeMeasures(measureParams, Some(batchId))
              })
          } else {
            executeMeasures(measureParams)
          }
        })
      })
  }

  /**
   * Performs a function with cached data source if necessary.
   * Caches data sources if it has more than 1 measure defined for it, and, `cacheDataSources` is `true`.
   * After the function is complete, the data source is uncached.
   *
   * @param dataSourceName name of data source
   * @param f function to perform
   * @param measureCountByDataSource number of measures for each data source
   * @return
   */
  private def withCacheIfNecessary(dataSourceName: String, f: => Unit)(
      implicit measureCountByDataSource: Map[String, Int]): Unit = {
    val numMeasures = measureCountByDataSource(dataSourceName)
    var isCached = false
    if (cacheDataSources && numMeasures > 1) {
      info(
        s"Caching data source with name '$dataSourceName' as $numMeasures measures are applied on it.")
      sparkSession.catalog.cacheTable(dataSourceName)
      isCached = true
    }

    f

    if (isCached) {
      sparkSession.catalog.uncacheTable(dataSourceName)
    }
  }

  /**
   * Executes measures for a data sources. Involves the following steps,
   *  - Transformation
   *  - Persist metrics if required
   *  - Persist records if required
   *
   *  All measures are executed in parallel.
   *
   * @param measureParams Object representation(s) of user defined measure(s).
   * @param batchId Option batch Id in case of streaming sources to identify micro batches.
   */
  private def executeMeasures(
      measureParams: Seq[MeasureParam],
      batchId: Option[Long] = None): Unit = {
    val batchDetailsOpt = batchId.map(bId => s"for batch id $bId").getOrElse(StringUtils.EMPTY)

    // define the tasks
    val tasks: Map[String, Future[_]] = (for (i <- measureParams.indices)
      yield {
        val measureParam = measureParams(i)
        val measureName = measureParam.getName

        (measureName, Future {
          val currentContext = context.cloneDQContext(ContextId(new Date().getTime))
          info(s"Started execution of measure with name '$measureName'")

          val measure = createMeasure(measureParam)
          val (recordsDf, metricsDf) = measure.execute(batchId)

          persistMetrics(currentContext, measure, metricsDf)
          persistRecords(currentContext, measure, recordsDf)

          MetricFlushStep(Some(measureParam)).execute(currentContext)
        })
      }).toMap

    tasks.foreach(task =>
      task._2.onComplete {
        case Success(_) =>
          info(s"Successfully executed measure with name '${task._1}' $batchDetailsOpt")
        case Failure(e) =>
          error(s"Error executing measure with name '${task._1}' $batchDetailsOpt", e)
      })

    Thread.sleep(1000)

    while (!tasks.forall(_._2.isCompleted)) {
      info(
        s"Measures with name ${tasks.filterNot(_._2.isCompleted).keys.mkString("['", "', '", "']")} " +
          s"are still executing.")
      Thread.sleep(1000)
    }

    info(
      s"Completed execution of all measures for data source with name '${measureParams.head.getDataSource}'.")
  }

  /**
   * Instantiates measure implementations based on the user defined configurations.
   *
   * @param measureParam Object representation of user defined a measure.
   * @return
   */
  private def createMeasure(measureParam: MeasureParam): Measure = {
    measureParam.getType match {
      case MeasureTypes.Completeness => CompletenessMeasure(sparkSession, measureParam)
      case MeasureTypes.Duplication => DuplicationMeasure(sparkSession, measureParam)
      case MeasureTypes.Profiling => ProfilingMeasure(sparkSession, measureParam)
      case MeasureTypes.Accuracy => AccuracyMeasure(sparkSession, measureParam)
      case MeasureTypes.SparkSQL => SparkSQLMeasure(sparkSession, measureParam)
      case MeasureTypes.SchemaConformance => SchemaConformanceMeasure(sparkSession, measureParam)
      case _ =>
        val errorMsg = s"Measure type '${measureParam.getType}' is not supported."
        val exception = new NotImplementedError(errorMsg)
        error(errorMsg, exception)
        throw exception
    }
  }

  /**
   * Persists records to one or more sink based on the user defined measure configuration.
   *
   * @param context DQ Context.
   * @param measure a measure implementation
   * @param recordsDf records dataframe to persist.
   */
  private def persistRecords(context: DQContext, measure: Measure, recordsDf: DataFrame): Unit = {
    val measureParam: MeasureParam = measure.measureParam

    measureParam.getOutputOpt(OutputType.RecordOutputType) match {
      case Some(_) =>
        if (measure.supportsRecordWrite) {
          recordsDf.createOrReplaceTempView("recordsDf")
          RecordWriteStep(measureParam.getName, "recordsDf").execute(context)
        } else warn(s"Measure with name '${measureParam.getName}' doesn't support record write")
      case None =>
    }
  }

  /**
   * Persists metrics to one or more sink based on the user defined measure configuration.
   *
   * @param context DQ Context.
   * @param measure a measure implementation
   * @param metricsDf metrics dataframe to persist
   */
  private def persistMetrics(context: DQContext, measure: Measure, metricsDf: DataFrame): Unit = {
    val measureParam: MeasureParam = measure.measureParam

    measureParam.getOutputOpt(OutputType.MetricOutputType) match {
      case Some(_) =>
        if (measure.supportsMetricWrite) {
          val metricDfName = s"${measureParam.getName}_metricsDf"
          metricsDf.createOrReplaceTempView(metricDfName)
          MetricWriteStep(measureParam.getName, metricDfName, DefaultFlattenType).execute(context)
        } else warn(s"Measure with name '${measureParam.getName}' doesn't support metric write")
      case None =>
    }
  }

}
