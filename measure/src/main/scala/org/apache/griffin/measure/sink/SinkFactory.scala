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

import org.apache.griffin.measure.configuration.dqdefinition.PersistParam

import scala.util.{Success, Try}


case class SinkFactory(persistParams: Iterable[PersistParam], metricName: String) extends Serializable {

  val HDFS_REGEX = """^(?i)hdfs$""".r
  val HTTP_REGEX = """^(?i)http$""".r
  val LOG_REGEX = """^(?i)log$""".r
  val MONGO_REGEX = """^(?i)mongo$""".r

  /**
    * create persist
    * @param timeStamp    the timestamp of persist
    * @param block        persist write metric in block or non-block way
    * @return   persist
    */
  def getPersists(timeStamp: Long, block: Boolean): MultiSinks = {
    MultiSinks(persistParams.flatMap(param => getPersist(timeStamp, param, block)))
  }

  private def getPersist(timeStamp: Long, persistParam: PersistParam, block: Boolean): Option[Sink] = {
    val config = persistParam.getConfig
    val persistTry = persistParam.getType match {
      case LOG_REGEX() => Try(ConsoleSink(config, metricName, timeStamp))
      case HDFS_REGEX() => Try(HdfsSink(config, metricName, timeStamp))
      case HTTP_REGEX() => Try(HttpSink(config, metricName, timeStamp, block))
      case MONGO_REGEX() => Try(MongoSink(config, metricName, timeStamp, block))
      case _ => throw new Exception("not supported persist type")
    }
    persistTry match {
      case Success(persist) if (persist.available) => Some(persist)
      case _ => None
    }
  }

}
