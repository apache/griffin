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

//import kafka.serializer.Decoder
import io.ebay.rheos.schema.event.RheosEvent
import org.apache.griffin.measure.config.params.user.DataConnectorParam
import org.apache.griffin.measure.process.engine.DqEngines
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.RheosUtils

import scala.util.{Failure, Success, Try}
import java.util.{Date, Properties}
import java.io.ByteArrayOutputStream

import com.ebay.crawler.streaming.rheos.utils.RheosEventCodec
import org.apache.avro.Schema
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class RheosStreamingDataConnector(sqlContext: SQLContext,
                                       @transient ssc: StreamingContext,
                                       dqEngines: DqEngines,
                                       dcParam: DataConnectorParam
                                      ) extends StreamingDataConnector {
//  type KD <: Decoder[K]
//  type VD <: Decoder[V]
  type K = Array[Byte]
  type V = RheosEvent
  type OUT = ConsumerRecord[K, V]

  val config = dcParam.config

  val KafkaConfig = "kafka.config"
  val CodecConfig = "codec.config"
  val Topics = "topics"
  val DataClass = "data.class"

  val kafkaConfig = config.get(KafkaConfig) match {
    case Some(map: Map[String, Any]) => map.mapValues(_.toString).map(identity)
    case _ => Map[String, String]()
  }
  val codecConfig = config.get(CodecConfig) match {
    case Some(map: Map[String, Any]) => map.mapValues(_.toString).map(identity)
    case _ => Map[String, String]()
  }
  val topics = config.getOrElse(Topics, "").toString

  val dataClassName = config.getOrElse(DataClass, "").toString
  val dataClass = try {
    Class.forName(dataClassName)
  } catch {
    case e: Throwable => {
      throw new Exception(s"data class param error")
    }
  }

  val properties = initProperties
  private def initProperties(): Properties = {
    val props = new Properties()
    codecConfig.foreach { pair =>
      val (k, v) = pair
      props.put(k, v)
    }
    props
  }

  val valueColName = "value"
  val schema = StructType(Array(
    StructField(valueColName, StringType)
  ))

  def available(): Boolean = {
    true
  }

  def init(): Unit = {
    // register fan in
    dataSourceCacheOpt.foreach(_.registerFanIn)

    val ds = stream match {
      case Success(dstream) => dstream
      case Failure(ex) => throw ex
    }
    ds.foreachRDD((rdd, time) => {
      val ms = time.milliseconds

      val t1 = new Date().getTime

      val saveDfOpt = try {
        // coalesce partition number
        val prlCount = rdd.sparkContext.defaultParallelism
//        val ptnCount = rdd.getNumPartitions
//        val repartitionedRdd = if (prlCount < ptnCount) {
////          rdd.coalesce(prlCount)
//          rdd.repartition(prlCount)
//        } else rdd
        val repartitionedRdd = rdd.repartition(prlCount)

        val cnt = rdd.count
        println(s"rheos receive data [${ms}] count: ${cnt}")

        val dfOpt = transform(repartitionedRdd)

        preProcess(dfOpt, ms)
      } catch {
        case e: Throwable => {
          error(s"streaming data connector error: ${e.getMessage}")
          None
        }
      }

      val t2 = new Date().getTime
      println(s"rheos transform time: ${t2 - t1} ms")

      // save data frame
      dataSourceCacheOpt.foreach(_.saveData(saveDfOpt, ms))

      val t3 = new Date().getTime
      println(s"rheos save time: ${t3 - t2} ms")
    })
  }

  def stream(): Try[InputDStream[OUT]] = Try {
    val topicSet = topics.split(",").toSet
    createDStream(topicSet)
  }

  protected def createDStream(topicSet: Set[String]): InputDStream[OUT] = {
    import scala.collection.JavaConversions._
    RheosUtils.createRheosDirectStream(ssc, kafkaConfig, topicSet)
  }

  def transform(rdd: RDD[OUT]): Option[DataFrame] = {
    // to reduce rdd partitions from rheos, to ignore multiple codec http request, which brings lots of exceptions.
//    val calcRdd = rdd.repartition(4)

    val rowRdd: RDD[Row] = rdd.mapPartitions { items =>
      val codec: RheosEventCodec = new RheosEventCodec(properties)
      val schema: Schema = ReflectData.get.getSchema(dataClass)
      items.flatMap { out =>
        try {
          val v = out.value
          val value = codec.decodeFromRheosEvent(v, dataClass)
          val msg = stringifyGenericRecord(value, schema)
          Some(Row(msg))
        } catch {
          case e: Throwable => None
        }
      }
    }

    rowRdd.cache

    val retDfOpt = if (rowRdd.isEmpty) None else {
      try {
        val df = sqlContext.createDataFrame(rowRdd, schema)
        Some(df)
      } catch {
        case e: Throwable => {
          error(s"streaming data transform fails")
          None
        }
      }
    }

    rowRdd.unpersist()

    retDfOpt
  }

  private def stringifyGenericRecord[T](record: T, schema: Schema): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val encoder: JsonEncoder = EncoderFactory.get.jsonEncoder(schema, out)
    val writer: ReflectDatumWriter[T] = new ReflectDatumWriter[T](schema)
    writer.write(record, encoder)
    encoder.flush()
    out.toString
  }

}