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
package org.apache.griffin.measure.data.connector

import kafka.serializer.StringDecoder
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.data.connector.streaming.{KafkaStreamingDataConnector, KafkaStreamingStringDataConnector, StreamingDataConnector}
import org.apache.griffin.measure.process.engine.{DqEngine, DqEngines}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Success
//import org.apache.griffin.measure.data.connector.cache._
import org.apache.griffin.measure.data.connector.batch._
//import org.apache.griffin.measure.data.connector.streaming._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag
import scala.util.Try

object DataConnectorFactory {

  val HiveRegex = """^(?i)hive$""".r
  val AvroRegex = """^(?i)avro$""".r
  val TextDirRegex = """^(?i)text-dir$""".r

  val KafkaRegex = """^(?i)kafka$""".r

  val TextRegex = """^(?i)text$""".r

  def getDataConnector(sqlContext: SQLContext,
                       @transient ssc: StreamingContext,
                       dqEngines: DqEngines,
                       dataConnectorParam: DataConnectorParam
                      ): Try[DataConnector] = {
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    val config = dataConnectorParam.config
    Try {
      conType match {
        case HiveRegex() => HiveBatchDataConnector(sqlContext, dqEngines, dataConnectorParam)
        case AvroRegex() => AvroBatchDataConnector(sqlContext, dqEngines, dataConnectorParam)
        case TextDirRegex() => TextDirBatchDataConnector(sqlContext, dqEngines, dataConnectorParam)
        case KafkaRegex() => {
//          val ksdcTry = getStreamingDataConnector(ssc, dataConnectorParam)
//          val cdcTry = getCacheDataConnector(sqlContext, dataConnectorParam.cache)
//          KafkaCacheDirectDataConnector(ksdcTry, cdcTry, dataConnectorParam)
          getStreamingDataConnector(sqlContext, ssc, dqEngines, dataConnectorParam)
        }
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

  private def getStreamingDataConnector(sqlContext: SQLContext,
                                        @transient ssc: StreamingContext,
                                        dqEngines: DqEngines,
                                        dataConnectorParam: DataConnectorParam
                                       ): StreamingDataConnector = {
    if (ssc == null) throw new Exception("streaming context is null!")
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    conType match {
      case KafkaRegex() => genKafkaDataConnector(sqlContext, ssc, dqEngines, dataConnectorParam)
      case _ => throw new Exception("streaming connector creation error!")
    }
  }
//
//  private def getCacheDataConnector(sqlContext: SQLContext,
//                                    dataCacheParam: DataCacheParam
//                                   ): Try[CacheDataConnector] = {
//    if (dataCacheParam == null) {
//      throw new Exception("invalid data cache param!")
//    }
//    val cacheType = dataCacheParam.cacheType
//    Try {
//      cacheType match {
//        case HiveRegex() => HiveCacheDataConnector(sqlContext, dataCacheParam)
//        case TextRegex() => TextCacheDataConnector(sqlContext, dataCacheParam)
//        case _ => throw new Exception("cache connector creation error!")
//      }
//    }
//  }
//
  private def genKafkaDataConnector(sqlContext: SQLContext,
                                    @transient ssc: StreamingContext,
                                    dqEngines: DqEngines,
                                    dataConnectorParam: DataConnectorParam
                                   ) = {
    val config = dataConnectorParam.config
    val KeyType = "key.type"
    val ValueType = "value.type"
    val keyType = config.getOrElse(KeyType, "java.lang.String").toString
    val valueType = config.getOrElse(ValueType, "java.lang.String").toString
    (getClassTag(keyType), getClassTag(valueType)) match {
      case (ClassTag(k: Class[String]), ClassTag(v: Class[String])) => {
        KafkaStreamingStringDataConnector(sqlContext, ssc, dqEngines, dataConnectorParam)
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

  def filterBatchDataConnectors(connectors: Seq[DataConnector]): Seq[BatchDataConnector] = {
    connectors.flatMap { dc =>
      dc match {
        case mdc: BatchDataConnector => Some(mdc)
        case _ => None
      }
    }
  }
  def filterStreamingDataConnectors(connectors: Seq[DataConnector]): Seq[StreamingDataConnector] = {
    connectors.flatMap { dc =>
      dc match {
        case mdc: StreamingDataConnector => Some(mdc)
        case _ => None
      }
    }
  }

}
