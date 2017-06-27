package org.apache.griffin.measure.connector

import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext

abstract class KafkaDataConnector(ssc: StreamingContext, config: Map[String, Any]
                                 ) extends StreamingDataConnector {
  type KD <: Decoder[K]
  type VD <: Decoder[V]

  val KafkaConfig = "kafka.config"
  val Topics = "topics"

  val kafkaConfig = config.get(KafkaConfig) match {
    case map: Map[String, Any] => map.mapValues(_.toString)
    case _ => Map[String, String]()
  }
  val topics = config.getOrElse(Topics, "").toString

  def available(): Boolean = {
    true
  }
}