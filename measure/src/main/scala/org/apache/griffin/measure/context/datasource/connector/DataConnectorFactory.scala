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
package org.apache.griffin.measure.context.datasource.connector

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.params.DataConnectorParam
import org.apache.griffin.measure.context.datasource.cache.DataSourceCache
import org.apache.griffin.measure.context.datasource.connector.batch._
import org.apache.griffin.measure.context.datasource.connector.streaming._
import org.apache.griffin.measure.context.datasource.info.TmstCache
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag
import scala.util.Try

object DataConnectorFactory extends Loggable {

  val HiveRegex = """^(?i)hive$""".r
  val AvroRegex = """^(?i)avro$""".r
  val TextDirRegex = """^(?i)text-dir$""".r

  val KafkaRegex = """^(?i)kafka$""".r

  /**
    * create data connector
    * @param sparkSession     spark env
    * @param ssc              spark streaming env
    * @param dcParam          data connector param
    * @param tmstCache        same tmst cache in one data source
    * @param dataSourceCacheOpt   for streaming data connector
    * @return   data connector
    */
  def getDataConnector(sparkSession: SparkSession,
                       ssc: StreamingContext,
                       dcParam: DataConnectorParam,
                       tmstCache: TmstCache,
                       dataSourceCacheOpt: Option[DataSourceCache]
                      ): Try[DataConnector] = {
    val conType = dcParam.conType
    val version = dcParam.version
    Try {
      conType match {
        case HiveRegex() => HiveBatchDataConnector(sparkSession, dcParam, tmstCache)
        case AvroRegex() => AvroBatchDataConnector(sparkSession, dcParam, tmstCache)
        case TextDirRegex() => TextDirBatchDataConnector(sparkSession, dcParam, tmstCache)
        case KafkaRegex() => {
          getStreamingDataConnector(sparkSession, ssc, dcParam, tmstCache, dataSourceCacheOpt)
        }
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

  private def getStreamingDataConnector(sparkSession: SparkSession,
                                        ssc: StreamingContext,
                                        dcParam: DataConnectorParam,
                                        tmstCache: TmstCache,
                                        dataSourceCacheOpt: Option[DataSourceCache]
                                       ): StreamingDataConnector = {
    if (ssc == null) throw new Exception("streaming context is null!")
    val conType = dcParam.conType
    val version = dcParam.version
    conType match {
      case KafkaRegex() => getKafkaDataConnector(sparkSession, ssc, dcParam, tmstCache, dataSourceCacheOpt)
      case _ => throw new Exception("streaming connector creation error!")
    }
  }

  private def getKafkaDataConnector(sparkSession: SparkSession,
                                    ssc: StreamingContext,
                                    dcParam: DataConnectorParam,
                                    tmstCache: TmstCache,
                                    dataSourceCacheOpt: Option[DataSourceCache]
                                   ): KafkaStreamingDataConnector  = {
    val KeyType = "key.type"
    val ValueType = "value.type"
    val config = dcParam.config
    val keyType = config.getOrElse(KeyType, "java.lang.String").toString
    val valueType = config.getOrElse(ValueType, "java.lang.String").toString
    (getClassTag(keyType), getClassTag(valueType)) match {
      case (ClassTag(k: Class[String]), ClassTag(v: Class[String])) => {
        KafkaStreamingStringDataConnector(sparkSession, ssc, dcParam, tmstCache, dataSourceCacheOpt)
      }
      case _ => {
        throw new Exception("not supported type kafka data connector")
      }
    }
  }

  private def getClassTag(tp: String): ClassTag[_] = {
    try {
      val clazz = Class.forName(tp)
      ClassTag(clazz)
    } catch {
      case e: Throwable => throw e
    }
  }

//  def filterDataConnectors[T <: DataConnector : ClassTag](connectors: Seq[DataConnector]): Seq[T] = {
//    connectors.flatMap { dc =>
//      dc match {
//        case mdc: T => Some(mdc)
//        case _ => None
//      }
//    }
//  }

}
