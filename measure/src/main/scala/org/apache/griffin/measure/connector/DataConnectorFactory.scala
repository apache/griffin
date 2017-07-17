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
package org.apache.griffin.measure.connector

import kafka.serializer.StringDecoder
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.rule.RuleExprs
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag
import scala.util.Try

object DataConnectorFactory {

  val HiveRegex = """^(?i)hive$""".r
  val AvroRegex = """^(?i)avro$""".r

  val KafkaRegex = """^(?i)kafka$""".r

  val DfRegex = """^(?i)df|dataframe$""".r

  def getDataConnector(sqlContext: SQLContext,
                       ssc: StreamingContext,
                       dataConnectorParam: DataConnectorParam,
                       ruleExprs: RuleExprs,
                       globalFinalCacheMap: Map[String, Any]
                      ): Try[BatchDataConnector] = {
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    val config = dataConnectorParam.config
    Try {
      conType match {
        case HiveRegex() => HiveBatchDataConnector(sqlContext, config, ruleExprs, globalFinalCacheMap)
        case AvroRegex() => AvroBatchDataConnector(sqlContext, config, ruleExprs, globalFinalCacheMap)
        case KafkaRegex() => KafkaDataConnector(sqlContext, ssc, dataConnectorParam, ruleExprs, globalFinalCacheMap)
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

  def getBatchDataConnector(sqlContext: SQLContext,
                            dataConnectorParam: DataConnectorParam,
                            ruleExprs: RuleExprs,
                            globalFinalCacheMap: Map[String, Any]
                           ): Try[BatchDataConnector] = {
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    val config = dataConnectorParam.config
    Try {
      conType match {
        case HiveRegex() => HiveBatchDataConnector(sqlContext, config, ruleExprs, globalFinalCacheMap)
        case AvroRegex() => AvroBatchDataConnector(sqlContext, config, ruleExprs, globalFinalCacheMap)
        case _ => throw new Exception("batch connector creation error!")
      }
    }
  }

  def getStreamingDataConnector(ssc: StreamingContext,
                                dataConnectorParam: DataConnectorParam
                               ): Try[StreamingDataConnector] = {
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    val config = dataConnectorParam.config
    Try {
      conType match {
        case KafkaRegex() => {
          genKafkaDataConnector(ssc, config)
        }
        case _ => throw new Exception("streaming connector creation error!")
      }
    }
  }

  def getCacheDataConnector(sqlContext: SQLContext,
                            dataCacheParam: DataCacheParam
                           ): Try[CacheDataConnector] = {
    if (dataCacheParam == null) {
      throw new Exception("invalid data cache param!")
    }
    val cacheType = dataCacheParam.cacheType
    Try {
      cacheType match {
        case DfRegex() => {
          DfCacheDataConnector(sqlContext, dataCacheParam)
        }
        case _ => throw new Exception("cache connector creation error!")
      }
    }
  }

  protected def genKafkaDataConnector(ssc: StreamingContext, config: Map[String, Any]) = {
    val KeyType = "key.type"
    val ValueType = "value.type"
    val keyType = config.getOrElse(KeyType, "java.lang.String").toString
    val valueType = config.getOrElse(ValueType, "java.lang.String").toString
//    val KafkaConfig = "kafka.config"
//    val Topics = "topics"
//    val kafkaConfig = config.get(KafkaConfig) match {
//      case Some(map: Map[String, Any]) => map.mapValues(_.toString).map(identity)
//      case _ => Map[String, String]()
//    }
//    val topics = config.getOrElse(Topics, "").toString
    (getClassTag(keyType), getClassTag(valueType)) match {
      case (ClassTag(k: Class[String]), ClassTag(v: Class[String])) => {
        new KafkaStreamingDataConnector(ssc, config) {
          type K = String
          type KD = StringDecoder
          type V = String
          type VD = StringDecoder
          def createDStream(topicSet: Set[String]): InputDStream[(K, V)] = {
            KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaConfig, topicSet)
          }
        }
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

}
