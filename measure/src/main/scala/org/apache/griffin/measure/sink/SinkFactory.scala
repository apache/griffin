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

import org.apache.griffin.measure.configuration.dqdefinition.SinkParam

import scala.util.{Success, Try}


case class SinkFactory(sinkParams: Iterable[SinkParam], metricName: String) extends Serializable {

  val HDFS_REGEX = """^(?i)hdfs$""".r
  val ELASTICSEARCH_REGEX = """^(?i)es|elasticsearch|http$""".r
  val CONSOLE_REGEX = """^(?i)console|log$""".r
  val MONGO_REGEX = """^(?i)mongo$""".r

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
      case CONSOLE_REGEX() => Try(ConsoleSink(config, metricName, timeStamp))
      case HDFS_REGEX() => Try(HdfsSink(config, metricName, timeStamp))
      case ELASTICSEARCH_REGEX() => Try(ElasticSearchSink(config, metricName, timeStamp, block))
      case MONGO_REGEX() => Try(MongoSink(config, metricName, timeStamp, block))
      case _ => throw new Exception(s"sink type ${sinkType} is not supported!")
    }
    sinkTry match {
      case Success(sink) if (sink.available) => Some(sink)
      case _ => None
    }
  }

}
