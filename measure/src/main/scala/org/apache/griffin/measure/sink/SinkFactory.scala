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

import scala.util.{Failure, Success, Try}

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.SinkParam
import org.apache.griffin.measure.configuration.enums.SinkType._
import org.apache.griffin.measure.utils.ParamUtil._

case class SinkFactory(sinkParamIter: Iterable[SinkParam], metricName: String)
    extends Loggable
    with Serializable {

  /**
   * create sink
   *
   * @param timeStamp the timestamp of sink
   * @param block     sink write metric in block or non-block way
   * @return sink
   */
  def getSinks(timeStamp: Long, block: Boolean): MultiSinks = {
    MultiSinks(sinkParamIter.flatMap(param => getSink(timeStamp, param, block)))
  }

  private def getSink(timeStamp: Long, sinkParam: SinkParam, block: Boolean): Option[Sink] = {
    val config = sinkParam.getConfig
    val sinkType = sinkParam.getType
    val sinkTry = sinkType match {
      case Console => Try(ConsoleSink(config, metricName, timeStamp))
      case Hdfs => Try(HdfsSink(config, metricName, timeStamp))
      case ElasticSearch => Try(ElasticSearchSink(config, metricName, timeStamp, block))
      case MongoDB => Try(MongoSink(config, metricName, timeStamp, block))
      case Custom => Try(getCustomSink(config, metricName, timeStamp, block))
      case _ => throw new Exception(s"sink type $sinkType is not supported!")
    }
    sinkTry match {
      case Success(sink) if sink.available() => Some(sink)
      case Failure(ex) =>
        error("Failed to get sink", ex)
        None
    }
  }

  /**
   * Using custom sink
   *
   * how it might look in env.json:
   *
   * "sinks": [
   * {
   * "type": "CUSTOM",
   * "config": {
   * "class": "com.yourcompany.griffin.sinks.MySuperSink",
   * "path": "/Users/Shared"
   * }
   * },
   *
   */
  private def getCustomSink(
      config: Map[String, Any],
      metricName: String,
      timeStamp: Long,
      block: Boolean): Sink = {
    val className = config.getString("class", "")
    val cls = Class.forName(className)
    if (classOf[Sink].isAssignableFrom(cls)) {
      val method = cls.getDeclaredMethod(
        "apply",
        classOf[Map[String, Any]],
        classOf[String],
        classOf[Long],
        classOf[Boolean])
      method
        .invoke(
          null,
          config,
          metricName.asInstanceOf[Object],
          timeStamp.asInstanceOf[Object],
          block.asInstanceOf[Object])
        .asInstanceOf[Sink]
    } else {
      throw new ClassCastException(s"$className should extend Sink")
    }
  }

}
