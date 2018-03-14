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

package org.apache.spark.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Created by jinxliu on 7/26/16.
  */
object WaltzConstant {
  val RheosKeyDeser = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
//  val RheosValueDeser = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
  val RheosValueDeser ="io.ebay.rheos.schema.avro.RheosEventDeserializer"

//  val RheosBootStrapServers = "rheos-kafka-proxy-1.lvs02.dev.ebayc3.com:9093,"  +
//  "rheos-kafka-proxy-2.lvs02.dev.ebayc3.com:9093," +
//    "rheos-kafka-proxy-3.lvs02.dev.ebayc3.com:9093," +
//    "rheos-kafka-proxy-1.phx02.dev.ebayc3.com:9093," +
//    "rheos-kafka-proxy-2.phx02.dev.ebayc3.com:9093," +
//    "rheos-kafka-proxy-3.phx02.dev.ebayc3.com:9093"

  val RheosBootStrapServers = "rheos-kafka-proxy-1.phx02.dev.ebayc3.com:9092,"  +
    "rheos-kafka-proxy-2.phx02.dev.ebayc3.com:9092," +
    "rheos-kafka-proxy-3.phx02.dev.ebayc3.com:9092," +
    "rheos-kafka-proxy-1.lvs02.dev.ebayc3.com:9092," +
    "rheos-kafka-proxy-2.lvs02.dev.ebayc3.com:9092," +
    "rheos-kafka-proxy-3.lvs02.dev.ebayc3.com:9092"

  val RheosSecParams = Map[String, String](
    "sasl.mechanism" -> "IAF",
    "security.protocol" -> "SASL_PLAINTEXT",
    "sasl.login.class" -> "io.ebay.rheos.kafka.security.iaf.IAFLogin",
    "sasl.callback.handler.class" -> "io.ebay.rheos.kafka.security.iaf.IAFCallbackHandler"
  )

  val RheosNeedAuth = "source.needAuth"
  val RheosMustHaveParams = List( ConsumerConfig.CLIENT_ID_CONFIG,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfig.GROUP_ID_CONFIG)
}
