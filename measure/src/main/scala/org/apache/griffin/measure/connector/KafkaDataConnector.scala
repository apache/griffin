package org.apache.griffin.measure.connector

import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}
import org.apache.griffin.measure.rule.RuleExprs
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag
import scala.util.Try

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

object KafkaTool {
  def generateKafkaDataConnector(ssc: StreamingContext, config: Map[String, Any]): KafkaDataConnector = {
    val KeyType = "key.type"
    val ValueType = "value.type"

    val keyType = config.getOrElse(KeyType, "java.lang.String").toString
    val valueType = config.getOrElse(ValueType, "java.lang.String").toString

    (getClassTag(keyType), getClassTag(valueType)) match {
      case (ClassTag(k: Class[String]), ClassTag(v: Class[String])) => {
        new KafkaDataConnector(ssc, config) {
          type K = String
          type KD = StringDecoder
          type V = String
          type VD = StringDecoder

          def stream(): Try[InputDStream[(K, V)]] = Try {
            val topicSet = topics.split(",").toSet
            KafkaUtils.createDirectStream[K, V, KD, VD](
              ssc,
              kafkaConfig,
              topicSet
            )
          }
        }
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