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

package org.apache.griffin.measure.job

import org.apache.spark.sql.AnalysisException
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.griffin.measure.Application.readParamFile
import org.apache.griffin.measure.configuration.dqdefinition.EnvConfig
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.step.builder.udf.GriffinUDFAgent

class BatchDQAppTest extends DQAppTest {

  override def beforeAll(): Unit = {
    super.beforeAll()

    envParam = readParamFile[EnvConfig](getConfigFilePath("/env-batch.json")) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }

    sparkParam = envParam.getSparkParam

    Try {
      sparkParam.getConfig.foreach { case (k, v) => spark.conf.set(k, v) }
      spark.conf.set("spark.app.name", "BatchDQApp Test")
      spark.conf.set("spark.sql.crossJoin.enabled", "true")

      val logLevel = getGriffinLogLevel
      sc.setLogLevel(sparkParam.getLogLevel)
      griffinLogger.setLevel(logLevel)

      // register udf
      GriffinUDFAgent.register(spark)
    }
  }

  def runAndCheckResult(metrics: Map[String, Any]): Unit = {
    dqApp.run match {
      case Success(ret) => assert(ret)
      case Failure(ex) =>
        error(s"process run error: ${ex.getMessage}", ex)
        throw ex
    }

    // check Result Metrics
    val dqContext = dqApp.asInstanceOf[BatchDQApp].dqContext
    val timestamp = dqContext.contextId.timestamp
    val expectedMetrics =
      Map(timestamp -> metrics)

    dqContext.metricWrapper.metrics should equal(expectedMetrics)
  }

  def runAndCheckException[T <: AnyRef](implicit classTag: ClassTag[T]): Unit = {
    dqApp.run match {
      case Success(_) =>
        fail(
          s"job ${dqApp.dqParam.getName} should not succeed, a ${classTag.toString} exception is expected.")
      case Failure(ex) => assertThrows[T](throw ex)
    }
  }

  "accuracy batch job" should "work" in {
    dqApp = initApp("/_accuracy-batch-griffindsl.json")
    val expectedMetrics = Map(
      "total_count" -> 50,
      "miss_count" -> 4,
      "matched_count" -> 46,
      "matchedFraction" -> 0.92)

    runAndCheckResult(expectedMetrics)
  }

  "completeness batch job" should "work" in {
    dqApp = initApp("/_completeness-batch-griffindsl.json")
    val expectedMetrics = Map("total" -> 50, "incomplete" -> 1, "complete" -> 49)

    runAndCheckResult(expectedMetrics)
  }

  "distinctness batch job" should "work" in {
    dqApp = initApp("/_distinctness-batch-griffindsl.json")

    val expectedMetrics =
      Map("total" -> 50, "distinct" -> 49, "dup" -> Seq(Map("dup" -> 1, "num" -> 1)))

    runAndCheckResult(expectedMetrics)
  }

  "profiling batch job" should "work" in {
    dqApp = initApp("/_profiling-batch-griffindsl.json")
    val expectedMetrics = Map(
      "prof" -> Seq(
        Map("user_id" -> 10004, "cnt" -> 1),
        Map("user_id" -> 10011, "cnt" -> 1),
        Map("user_id" -> 10010, "cnt" -> 1),
        Map("user_id" -> 10002, "cnt" -> 1),
        Map("user_id" -> 10006, "cnt" -> 1),
        Map("user_id" -> 10001, "cnt" -> 1),
        Map("user_id" -> 10005, "cnt" -> 1),
        Map("user_id" -> 10008, "cnt" -> 1),
        Map("user_id" -> 10013, "cnt" -> 1),
        Map("user_id" -> 10003, "cnt" -> 1),
        Map("user_id" -> 10007, "cnt" -> 1),
        Map("user_id" -> 10012, "cnt" -> 1),
        Map("user_id" -> 10009, "cnt" -> 1)),
      "post_group" -> Seq(Map("post_code" -> "94022", "cnt" -> 13)))

    runAndCheckResult(expectedMetrics)
  }

  "timeliness batch job" should "work" in {
    dqApp = initApp("/_timeliness-batch-griffindsl.json")
    val expectedMetrics = Map(
      "total" -> 10,
      "avg" -> 276000,
      "percentile_95" -> 660000,
      "step" -> Seq(
        Map("step" -> 0, "cnt" -> 6),
        Map("step" -> 5, "cnt" -> 2),
        Map("step" -> 3, "cnt" -> 1),
        Map("step" -> 4, "cnt" -> 1)))

    runAndCheckResult(expectedMetrics)
  }

  "uniqueness batch job" should "work" in {
    dqApp = initApp("/_uniqueness-batch-griffindsl.json")
    val expectedMetrics = Map("total" -> 50, "unique" -> 48)

    runAndCheckResult(expectedMetrics)
  }

  "batch job" should "fail with exception caught due to invalid rules" in {
    dqApp = initApp("/_profiling-batch-griffindsl_malformed.json")

    runAndCheckException[AnalysisException]
  }
}
