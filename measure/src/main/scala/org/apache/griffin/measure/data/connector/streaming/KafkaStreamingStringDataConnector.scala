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
package org.apache.griffin.measure.data.connector.streaming

import kafka.serializer.StringDecoder
import org.apache.griffin.measure.config.params.user.DataConnectorParam
import org.apache.griffin.measure.process.engine.DqEngines
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.functions.lit

case class KafkaStreamingStringDataConnector(sqlContext: SQLContext,
                                             @transient ssc: StreamingContext,
                                             dqEngines: DqEngines,
                                             dcParam: DataConnectorParam
                                            ) extends KafkaStreamingDataConnector {
  type K = String
  type KD = StringDecoder
  type V = String
  type VD = StringDecoder

  val valueColName = "value"
  val schema = StructType(Array(
    StructField(valueColName, StringType)
  ))

  def createDStream(topicSet: Set[String]): InputDStream[(K, V)] = {
    KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaConfig, topicSet)
  }

  def transform(rdd: RDD[(K, V)]): Option[DataFrame] = {
    if (rdd.isEmpty) None else {
      try {
        val rowRdd = rdd.map(d => Row(d._2))
        val df = sqlContext.createDataFrame(rowRdd, schema)
        Some(df)
      } catch {
        case e: Throwable => {
          error(s"streaming data transform fails")
          None
        }
      }
    }
  }
}
