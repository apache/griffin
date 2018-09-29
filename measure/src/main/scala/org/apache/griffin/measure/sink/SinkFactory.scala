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
package org.apache.griffin.measure.sink

import scala.util.{Success, Try}

import org.apache.griffin.measure.configuration.dqdefinition.SinkParam
import org.apache.griffin.measure.configuration.enums._



case class SinkFactory(sinkParams: Iterable[SinkParam], metricName: String) extends Serializable {

  /**
    * create sink
    * @param timeStamp    the timestamp of sink
    * @param block        sink write metric in block or non-block way
    * @return   sink
    */
  def getSinks(timeStamp: Long, block: Boolean): MultiSinks = {
    MultiSinks(sinkParams.flatMap(param => getSink(timeStamp, param, block)))
  }

  private def getSink(timeStamp: Long, sinkParam: SinkParam, block: Boolean): Option[Sink] = {
    val config = sinkParam.getConfig
    val sinkType = sinkParam.getType
    val sinkTry = sinkType match {
      case ConsoleSinkType => Try(ConsoleSink(config, metricName, timeStamp))
      case HdfsSinkType => Try(HdfsSink(config, metricName, timeStamp))
      case ElasticsearchSinkType => Try(ElasticSearchSink(config, metricName, timeStamp, block))
      case MongoSinkType => Try(MongoSink(config, metricName, timeStamp, block))
      case _ => throw new Exception(s"sink type ${sinkType} is not supported!")
    }
    sinkTry match {
      case Success(sink) if (sink.available) => Some(sink)
      case _ => None
    }
  }

}
