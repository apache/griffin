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

package org.apache.griffin.measure.sink

import scala.collection.mutable

import org.apache.griffin.measure.configuration.dqdefinition.{RuleOutputParam, SinkParam}
import org.apache.griffin.measure.configuration.enums.FlattenType.DefaultFlattenType
import org.apache.griffin.measure.step.write.{MetricFlushStep, MetricWriteStep, RecordWriteStep}

class CustomSinkTest extends SinkTestBase {

  val sinkParam: SinkParam =
    SinkParam(
      "customSink",
      "custom",
      Map("class" -> "org.apache.griffin.measure.sink.CustomSink"))
  override var sinkParams = Seq(sinkParam)

  def withCustomSink[A](func: Iterable[Sink] => A): A = {
    val sinkFactory = SinkFactory(sinkParams, "Test Sink Factory")
    val timestamp = System.currentTimeMillis
    val sinks = sinkFactory.getSinks(timestamp, block = true)
    func(sinks)
  }

  "custom sink" can "sink metrics" in {
    val actualMetrics = withCustomSink(sinks => {
      sinks.foreach { sink =>
        try {
          sink.sinkMetrics(Map("sum" -> 10))
        } catch {
          case e: Throwable => error(s"sink metrics error: ${e.getMessage}", e)
        }
      }
      sinks.foreach { sink =>
        try {
          sink.sinkMetrics(Map("count" -> 5))
        } catch {
          case e: Throwable => error(s"sink metrics error: ${e.getMessage}", e)
        }
      }
      sinks.headOption match {
        case Some(sink: CustomSink) => sink.allMetrics
        case _ => mutable.ListBuffer[String]()
      }
    })

    val expected = Map("sum" -> 10, "count" -> 5)
    actualMetrics should be(expected)
  }

  "custom sink" can "sink records" in {
    val actualRecords = withCustomSink(sinks => {
      val rdd1 = createDataFrame(1 to 2)
      sinks.foreach { sink =>
        try {
          sink.sinkRecords(rdd1.toJSON.rdd, "test records")
        } catch {
          case e: Throwable => error(s"sink records error: ${e.getMessage}", e)
        }
      }
      val rdd2 = createDataFrame(2 to 4)
      sinks.foreach { sink =>
        try {
          sink.sinkRecords(rdd2.toJSON.rdd, "test records")
        } catch {
          case e: Throwable => error(s"sink records error: ${e.getMessage}", e)
        }
      }
      sinks.headOption match {
        case Some(sink: CustomSink) => sink.allRecords
        case _ =>
      }
    })

    val expected = List(
      "{\"id\":1,\"name\":\"name_1\",\"sex\":\"women\",\"age\":16}",
      "{\"id\":2,\"name\":\"name_2\",\"sex\":\"man\",\"age\":17}",
      "{\"id\":2,\"name\":\"name_2\",\"sex\":\"man\",\"age\":17}",
      "{\"id\":3,\"name\":\"name_3\",\"sex\":\"women\",\"age\":18}",
      "{\"id\":4,\"name\":\"name_4\",\"sex\":\"man\",\"age\":19}")

    actualRecords should be(expected)
  }

  val metricsDefaultOutput: RuleOutputParam =
    RuleOutputParam("metrics", "default_output", "default")

  "RecordWriteStep" should "work with custom sink" in {
    val resultTable = "result_table"
    val df = createDataFrame(1 to 5)
    df.createOrReplaceTempView(resultTable)

    val rwName = Some(metricsDefaultOutput).flatMap(_.getNameOpt).getOrElse(resultTable)
    val dQContext = getDqContext()
    RecordWriteStep(rwName, resultTable).execute(dQContext)

    val actualRecords = dQContext.getSinks.headOption match {
      case Some(sink: CustomSink) => sink.allRecords
      case _ => mutable.ListBuffer[String]()
    }

    val expected = List(
      "{\"id\":1,\"name\":\"name_1\",\"sex\":\"women\",\"age\":16}",
      "{\"id\":2,\"name\":\"name_2\",\"sex\":\"man\",\"age\":17}",
      "{\"id\":3,\"name\":\"name_3\",\"sex\":\"women\",\"age\":18}",
      "{\"id\":4,\"name\":\"name_4\",\"sex\":\"man\",\"age\":19}",
      "{\"id\":5,\"name\":\"name_5\",\"sex\":\"women\",\"age\":20}")

    actualRecords should be(expected)
  }

  val metricsEntriesOutput: RuleOutputParam =
    RuleOutputParam("metrics", "entries_output", "entries")
  val metricsArrayOutput: RuleOutputParam = RuleOutputParam("metrics", "array_output", "array")
  val metricsMapOutput: RuleOutputParam = RuleOutputParam("metrics", "map_output", "map")

  "MetricWriteStep" should "output default metrics with custom sink" in {
    val resultTable = "result_table"
    val df = createDataFrame(1 to 5)
    df.groupBy("sex")
      .agg("age" -> "max", "age" -> "avg")
      .createOrReplaceTempView(resultTable)

    val dQContext = getDqContext()

    val metricWriteStep = {
      val metricOpt = Some(metricsDefaultOutput)
      val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse("default_metrics_name")
      val flattenType = metricOpt.map(_.getFlatten).getOrElse(DefaultFlattenType)
      MetricWriteStep(mwName, resultTable, flattenType)
    }

    metricWriteStep.execute(dQContext)
    MetricFlushStep().execute(dQContext)
    val actualMetrics = dQContext.getSinks.headOption match {
      case Some(sink: CustomSink) => sink.allMetrics
      case _ => mutable.Map[String, Any]()
    }

    val metricsValue = Seq(
      Map("sex" -> "man", "max(age)" -> 19, "avg(age)" -> 18.0),
      Map("sex" -> "women", "max(age)" -> 20, "avg(age)" -> 18.0))

    val expected = Map("default_output" -> metricsValue)

    actualMetrics("value") should be(expected)
  }

}
