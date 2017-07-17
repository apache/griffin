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

import java.util.Date
import java.util.concurrent.TimeUnit

import kafka.serializer.StringDecoder
import org.apache.griffin.measure.algo.streaming.StreamingProcess
import org.apache.griffin.measure.cache.info.InfoCacheInstance
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user.{DataConnectorParam, EvaluateRuleParam}
import org.apache.griffin.measure.config.reader.ParamRawStringReader
import org.apache.griffin.measure.result.{DataInfo, TimeStampInfo}
import org.apache.griffin.measure.rule.expr.{Expr, StatementExpr}
import org.apache.griffin.measure.rule._
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.griffin.measure.rule.{DataTypeCalculationUtil, ExprValueUtil, RuleExprs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class ConnectorTest extends FunSuite with Matchers with BeforeAndAfter {

  test("read config") {

    val a = "java.lang.String"
    val at = getClassTag(a)
    println(at)

    at match {
      case ClassTag(m) => println(m)
      case _ => println("no")
    }

  }

  private def getClassTag(tp: String): ClassTag[_] = {
    val clazz = Class.forName(tp)
    ClassTag(clazz)
  }

//  private def getDeserializer(ct: ClassTag[_]): String = {
//    ct.runtimeClass.get
//    ct match {
//      case Some(t: scala.Predef.Class[String]) => "kafka.serializer.StringDecoder"
//    }
//  }

//  "config": {
//    "kafka.config": {
//    "bootstrap.servers": "localhost:9092",
//    "group.id": "group1",
//    "auto.offset.reset": "smallest",
//    "auto.commit.enable": "false",
//  },
//    "topics": "sss",
//    "key.type": "java.lang.String",
//    "value.type": "java.lang.String",
//    "cache": {
//    "type": "temp",
//    "config": {
//    "table.name": "source",
//    "info.path": "src"
//  }
//  }
//  }

  test("connector") {
    val kafkaConfig = Map[String, String](
      ("bootstrap.servers" -> "10.149.247.156:9092"),
      ("group.id" -> "test"),
      ("auto.offset.reset" -> "smallest"),
      ("auto.commit.enable" -> "false")
    )

    val cacheConfig = Map[String, Any](
      ("table.name" -> "source"),
      ("info.path" -> "src")
    )

    val cacheParam = Map[String, Any](
      ("type" -> "df"),
      ("config" -> cacheConfig)
    )

    val config = Map[String, Any](
      ("kafka.config" -> kafkaConfig),
      ("topics" -> "sss"),
      ("key.type" -> "java.lang.String"),
      ("value.type" -> "java.lang.String"),
      ("cache" -> cacheParam)
    )

    val infoCacheConfig = Map[String, Any](
      ("hosts" -> "localhost:2181"),
      ("namespace" -> "griffin/infocache"),
      ("lock.path" -> "lock"),
      ("mode" -> "persist"),
      ("init.clear" -> true),
      ("close.clear" -> false)
    )
    val name = "ttt"

    val icp = InfoCacheParam("zk", infoCacheConfig)
    val icps = icp :: Nil

    InfoCacheInstance.initInstance(icps, name)
    InfoCacheInstance.init


    val connectorParam = DataConnectorParam("kafka", "0.8", config)

    val conf = new SparkConf().setMaster("local[*]").setAppName("ConnectorTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)

    val batchInterval = TimeUtil.milliseconds("2s") match {
      case Some(interval) => Milliseconds(interval)
      case _ => throw new Exception("invalid batch interval")
    }
    val ssc = new StreamingContext(sc, batchInterval)
    ssc.checkpoint("/test/griffin/cp")

    val connector = DataConnectorFactory.getStreamingDataConnector(ssc, connectorParam)

    val streamingConnector = connector match {
      case Success(c) => c
      case _ => fail
    }

    val cacheDataConnectorParam = connectorParam.config.get("cache") match {
      case Some(map: Map[String, Any]) => DataConnectorParam(map)
      case _ => throw new Exception("invalid cache parameter!")
    }
    val cacheDataConnector = DataConnectorFactory.getCacheDataConnector(sqlContext, cacheDataConnectorParam) match {
      case Success(cntr) => cntr
      case Failure(ex) => throw ex
    }

    ///

    def genDataFrame(rdd: RDD[Map[String, Any]]): DataFrame = {
      val fields = rdd.aggregate(Map[String, DataType]())(
        DataTypeCalculationUtil.sequenceDataTypeMap, DataTypeCalculationUtil.combineDataTypeMap
      ).toList.map(f => StructField(f._1, f._2))
      val schema = StructType(fields)
      val datas: RDD[Row] = rdd.map { d =>
        val values = fields.map { field =>
          val StructField(k, dt, _, _) = field
          d.get(k) match {
            case Some(v) => v
            case _ => null
          }
        }
        Row(values: _*)
      }
      val df = sqlContext.createDataFrame(datas, schema)
      df
    }

    val rules = "$source.json().name = 's2' AND $source.json().age = 32"
    val ep = EvaluateRuleParam(1, rules)

    val ruleFactory = RuleFactory(ep)
    val rule: StatementExpr = ruleFactory.generateRule()
    val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

    val ruleExprs = ruleAnalyzer.sourceRuleExprs
    val constFinalExprValueMap = Map[String, Any]()

    ///

    val ds = streamingConnector.stream match {
      case Success(dstream) => dstream
      case Failure(ex) => throw ex
    }

    ds.foreachRDD((rdd, time) => {
      val ms = time.milliseconds

      val data = rdd.collect
      val str = data.mkString("\n")

      println(s"${ms}: \n${str}")

      val dataInfoMap = DataInfo.cacheInfoList.map(_.defWrap).toMap + TimeStampInfo.wrap(ms)

      // parse each message
      val valueMapRdd: RDD[Map[String, Any]] = rdd.flatMap { kv =>
        val msg = kv._2

        val cacheExprValueMaps = ExprValueUtil.genExprValueMaps(Some(msg), ruleExprs.cacheExprs, constFinalExprValueMap)
        val finalExprValueMaps = ExprValueUtil.updateExprValueMaps(ruleExprs.finalCacheExprs, cacheExprValueMaps)

        //        val sf = StructField("name", DataType.fromJson("string"))
        //        val schema: StructType = new StructType()

        finalExprValueMaps.map { vm =>
          vm ++ dataInfoMap
        }
      }

      val cnt = valueMapRdd.count

      val valueMaps = valueMapRdd.collect()
      val valuestr = valueMaps.mkString("\n")

//      println(s"count: ${cnt}\n${valuestr}")

      // generate DataFrame
      val df = genDataFrame(valueMapRdd)
//      df.show(10)

      // save data frame
      cacheDataConnector.saveData(df, ms)

      // show data
//      cacheDataConnector.readData() match {
//        case Success(rdf) => rdf.show(10)
//        case Failure(ex) => println(s"cache data error: ${ex.getMessage}")
//      }
//
//      cacheDataConnector.submitLastProcTime(ms)
    })

    // process thread
    case class Process() extends Runnable {
      val lock = InfoCacheInstance.genLock("process")
      def run(): Unit = {
        val locked = lock.lock(5, TimeUnit.SECONDS)
        if (locked) {
          try {
            // show data
            cacheDataConnector.readData() match {
              case Success(rdf) => {
                rdf.show(10)
                println(s"count: ${rdf.count}")
              }
              case Failure(ex) => println(s"cache data error: ${ex.getMessage}")
            }

//            val st = new Date().getTime
            // get data
//            val sourceData = sourceDataConnector.data match {
//              case Success(dt) => dt
//              case Failure(ex) => throw ex
//            }
//            val targetData = targetDataConnector.data match {
//              case Success(dt) => dt
//              case Failure(ex) => throw ex
//            }
//
//            // accuracy algorithm
//            val (accuResult, missingRdd, matchedRdd) = accuracy(sourceData, targetData, ruleAnalyzer)
//
//            println(accuResult)
//
//            val et = new Date().getTime
//
//            val missingRecords = missingRdd.map(record2String(_, ruleAnalyzer.sourceRuleExprs.persistExprs, ruleAnalyzer.targetRuleExprs.persistExprs))

          } finally {
            lock.unlock()
          }
        }
      }
    }

    val processInterval = TimeUtil.milliseconds("10s") match {
      case Some(interval) => interval
      case _ => throw new Exception("invalid batch interval")
    }
    val process = StreamingProcess(processInterval, Process())

    process.startup()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true, stopGracefully=true)

    // context stop
    sc.stop

    InfoCacheInstance.close()

    process.shutdown()

  }



}

