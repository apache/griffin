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

import java.{util => ju}

import io.ebay.rheos.schema.event.RheosEvent
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function.{Function0 => JFunction0}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream._
import org.apache.spark.{Logging, SparkContext}

import scala.collection.JavaConverters._

/**
 * :: Experimental ::
 * object for constructing Kafka streams and RDDs
 */
@Experimental
object RheosUtils extends Logging {
  /**
   * :: Experimental ::
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
    *
    * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): RDD[ConsumerRecord[K, V]] = {
    val preferredHosts = locationStrategy match {
      case PreferBrokers =>
        throw new AssertionError(
          "If you want to prefer brokers, you must provide a mapping using PreferFixed " +
          "A single RheosRDD does not have a driver consumer and cannot look up brokers for you.")
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()

    new RheosRDD[K, V](sc, kp, osr, preferredHosts, true)
  }

  /**
   * :: Experimental ::
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
    *
    * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): JavaRDD[ConsumerRecord[K, V]] = {

    new JavaRDD(createRDD[K, V](jsc.sc, kafkaParams, offsetRanges, locationStrategy))
  }

  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
   *  of messages
   * per second that each '''partition''' will accept.
    *
    * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): InputDStream[ConsumerRecord[K, V]] = {
    new DirectRheosInputDStream[K, V](ssc, locationStrategy, consumerStrategy)
  }

  private def fixKafkaParamsForRheos(
       kafkaParams: ju.Map[String, Object]
    ): Unit = {
    // check whether must-have params are set
//    for ( param <- WaltzConstant.RheosMustHaveParams) {
//      if (! kafkaParams.containsKey(param) || kafkaParams.get(param).toString.isEmpty) {
//        throw new RuntimeException(s"invalid rheos config: $param is not set.")
//      }
//    }
//    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, WaltzConstant.RheosBootStrapServers)
//    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, WaltzConstant.RheosKeyDeser)
//    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WaltzConstant.RheosValueDeser)

    // check whether need to enable security
//    if (! kafkaParams.containsKey(WaltzConstant.RheosNeedAuth)
//      || kafkaParams.get(WaltzConstant.RheosNeedAuth).toString.equals("1")) {
//      for ((key, value) <- WaltzConstant.RheosSecParams) {
//        kafkaParams.put(key.toString, value.toString)
//      }
//    }
    /* val config: ju.Map[String, AnyRef] = new ju.HashMap[String, AnyRef]
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "http://rheos-services.qa.ebay.com")

    val connector: KafkaConsumerConnector = new DataStreamKafkaConsumerConnector(config)
    val consumerName = kafkaParams.get("source.rheos.consumer.name").toString
    val kafkaConsumer = kafkaParams.get("useRheosEvent").toString match {
      case "0" => connector.createByteArrayTypedKafkaConsumer(consumerName)
      case "1" => connector.createRheosEventTypedKafkaConsumer(consumerName)
    }
    // scalastyle:on
    kafkaConsumer.asInstanceOf[KafkaConsumer[Array[Byte], Array[Byte]]] */
  }

  /**
    * :: Experimental ::
    * Scala constructor for a DStream where
    * each given Kafka topic/partition corresponds to an RDD partition.
    * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
    *  of messages
    * per second that each '''partition''' will accept.
    */
  @Experimental
  def createRheosDirectStream(
                                ssc: StreamingContext,
                                kafkaParams: ju.Map[String, Object],
                                topics: Set[String]
                              ): InputDStream[ConsumerRecord[Array[Byte], RheosEvent]] = {
    try {
      fixKafkaParamsForRheos(kafkaParams)
    } catch {
      case runtime : RuntimeException => {
        logError(runtime.getMessage)
        throw new RuntimeException("Cannot create rheos stream due to invalid config")
      }
    }
    val rheosConsumer = new KafkaConsumer[Array[Byte], RheosEvent](kafkaParams)
    val assignedTps = topics.flatMap(topic => rheosConsumer.partitionsFor(topic).toArray)
      .asInstanceOf[Set[PartitionInfo]]
      .map({ pi =>
        new TopicPartition(pi.topic(), pi.partition())
      })
    new DirectRheosInputDStream[Array[Byte], RheosEvent](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign(assignedTps, kafkaParams.asScala))
  }

  /**
   * :: Experimental ::
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
    *
    * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy))
  }

  /**
   * Tweak kafka params to prevent issues on executors
   */
  private[kafka] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {
    logWarning(s"overriding ${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} to false for executor")
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    /* logWarning(s"overriding ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} to none for executor")
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none") */

    // driver and executor should be in different consumer groups
    /* val originalGroupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    if (null == originalGroupId) {
      logError(s"${ConsumerConfig.GROUP_ID_CONFIG} is null, you should probably set it")
    }
    val groupId = "spark-executor-" + originalGroupId
    logWarning(s"overriding executor ${ConsumerConfig.GROUP_ID_CONFIG} to ${groupId}")
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId) */

    // possible workaround for KAFKA-3135
    val rbb = kafkaParams.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG)
    if (null == rbb || rbb.asInstanceOf[java.lang.String].toInt < 65536) {
      logWarning(s"overriding ${ConsumerConfig.RECEIVE_BUFFER_CONFIG} to 65536 see KAFKA-3135")
      kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
    }
  }
}
