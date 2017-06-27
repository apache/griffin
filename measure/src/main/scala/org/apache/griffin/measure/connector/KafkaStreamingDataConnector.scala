///*-
// * Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//
// */
//package org.apache.griffin.measure.connector
//
//import kafka.serializer._
//import org.apache.griffin.measure.rule.RuleExprs
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//
//import scala.reflect.ClassTag
//
//import scala.util.Try
//
//
//case class KafkaStreamingDataConnector(ssc: StreamingContext, config: Map[String, Any],
//                                       ruleExprs: RuleExprs, constFinalExprValueMap: Map[String, Any]
//                                      ) extends StreamingDataConnector {
//
//  val KafkaConfig = "kafka.config"
//  val Topics = "topics"
//  val KeyType = "key.type"
//  val KeyDeserializer = "key.deserializer"
//  val ValueType = "value.type"
//  val ValueDeserializer = "value.deserializer"
//
//  val kafkaConfig = config.get(KafkaConfig) match {
//    case map: Map[String, Any] => map.mapValues(_.toString)
//    case _ => Map[String, String]()
//  }
//  val topics = config.getOrElse(Topics, "").toString
//
//  val keyType = config.getOrElse(KeyType, "java.lang.String").toString
//  val valueType = config.getOrElse(ValueType, "java.lang.String").toString
//
////  import scala.reflect.runtime.universe._
////  val m = runtimeMirror(getClass.getClassLoader)
////  val classSymbol = m.staticClass("java.lang.String")
////  val tpe = classSymbol.selfType
//
//  def available(): Boolean = {
//    true
//  }
//
//  def stream(): Try[InputDStream[(_, _)]] = Try {
//    val topicSet = topics.split(",").toSet
//    val keyClassTag = getClassTag(keyType)
//    val valueClassTag = getClassTag(valueType)
//    KafkaUtils.createDirectStream(
//      ssc,
//      kafkaConfig,
//      topicSet
//    )(keyClassTag, valueClassTag, getDeserializer(keyClassTag), getDeserializer(valueClassTag))
//  }
//
//  private def getClassTag(tp: String): ClassTag[_] = {
//    try {
//      val clazz = Class.forName(tp)
//      ClassTag(clazz)
//    } catch {
//      case e: Throwable => throw e
//    }
//
//  }
//
//  private def getDeserializer[T](ct: ClassTag[T]): ClassTag[Decoder[T]] = {
//    try {
//      ct match {
//        case ClassTag(clz: Class[String]) => ClassTag(classOf[StringDecoder])
//        case _ => ClassTag(classOf[DefaultDecoder])
//      }
//    } catch {
//      case e: Throwable => throw e
//    }
//  }
//
//}
