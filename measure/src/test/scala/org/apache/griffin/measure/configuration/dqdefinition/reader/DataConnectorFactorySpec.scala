/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.configuration.dqdefinition.reader

import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.FlatSpec

import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.datasource.cache.StreamingCacheClient
import org.apache.griffin.measure.datasource.connector.DataConnectorFactory
import org.apache.griffin.measure.datasource.connector.batch.{BatchDataConnector, BatchDataConnectorContext}
import org.apache.griffin.measure.datasource.connector.streaming.{StreamingDataConnector, StreamingDataConnectorContext}

case class ExampleBatchDataConnector(ctx: BatchDataConnectorContext) extends BatchDataConnector {
  override val sparkSession: SparkSession = ctx.sparkSession
  override val dcParam: DataConnectorParam = ctx.dcParam
  override val timestampStorage: TimestampStorage = ctx.timestampStorage

  override def data(ms: Long): (Option[DataFrame], TimeRange) = (None, TimeRange(ms))
}


case class ExampleStreamingDataConnector(ctx: StreamingDataConnectorContext) extends StreamingDataConnector {
  override type K = Unit
  override type V = Unit
  override type OUT = Unit

  override protected def stream(): Try[InputDStream[this.OUT]] = null

  override def transform(rdd: RDD[this.OUT]): Option[DataFrame] = None

  override val streamingCacheClientOpt: Option[StreamingCacheClient] = ctx.streamingCacheClientOpt
  override val sparkSession: SparkSession = ctx.sparkSession
  override val dcParam: DataConnectorParam = ctx.dcParam
  override val timestampStorage: TimestampStorage = ctx.timestampStorage

  override def init(): Unit = ()
}

class NotDataConnector


class DataConnectorWithoutApply extends BatchDataConnector {
  override val sparkSession: SparkSession = null
  override val dcParam: DataConnectorParam = null
  override val timestampStorage: TimestampStorage = null

  override def data(ms: Long): (Option[DataFrame], TimeRange) = null
}


class DataConnectorFactorySpec extends FlatSpec {

  "DataConnectorFactory" should "be able to create custom batch connector" in {
    val param = DataConnectorParam(
      "CUSTOM", null, null,
      Map("class" -> classOf[ExampleBatchDataConnector].getCanonicalName), Nil)
    // apparently Scalamock can not mock classes without empty-paren constructor, providing nulls
    val res = DataConnectorFactory.getDataConnector(
      null, null, param, null, None)
    assert(res.get != null)
    assert(res.isSuccess)
    assert(res.get.isInstanceOf[ExampleBatchDataConnector])
    assert(res.get.data(42)._2.begin == 42)
  }

  it should "be able to create custom streaming connector" in {
    val param = DataConnectorParam(
      "CUSTOM", null, null,
      Map("class" -> classOf[ExampleStreamingDataConnector].getCanonicalName), Nil)
    // apparently Scalamock can not mock classes without empty-paren constructor, providing nulls
    val res = DataConnectorFactory.getDataConnector(
      null, null, param, null, None)
    assert(res.isSuccess)
    assert(res.get.isInstanceOf[ExampleStreamingDataConnector])
    assert(res.get.data(0)._2 == TimeRange.emptyTimeRange)
  }

  it should "fail if class is not extending DataConnectors" in {
    val param = DataConnectorParam(
      "CUSTOM", null, null,
      Map("class" -> classOf[NotDataConnector].getCanonicalName), Nil)
    // apparently Scalamock can not mock classes without empty-paren constructor, providing nulls
    val res = DataConnectorFactory.getDataConnector(
      null, null, param, null, None)
    assert(res.isFailure)
    assert(res.failed.get.isInstanceOf[ClassCastException])
    assert(res.failed.get.getMessage ==
      "org.apache.griffin.measure.configuration.dqdefinition.reader.NotDataConnector" +
        " should extend BatchDataConnector or StreamingDataConnector")
  }

  it should "fail if class does not have apply() method" in {
    val param = DataConnectorParam(
      "CUSTOM", null, null,
      Map("class" -> classOf[DataConnectorWithoutApply].getCanonicalName), Nil)
    // apparently Scalamock can not mock classes without empty-paren constructor, providing nulls
    val res = DataConnectorFactory.getDataConnector(
      null, null, param, null, None)
    assert(res.isFailure)
    assert(res.failed.get.isInstanceOf[NoSuchMethodException])
    assert(res.failed.get.getMessage ==
      "org.apache.griffin.measure.configuration.dqdefinition.reader.DataConnectorWithoutApply" +
        ".apply(org.apache.griffin.measure.datasource.connector.batch.BatchDataConnectorContext)")
  }

}
