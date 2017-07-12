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
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user.{DataConnectorParam, EvaluateRuleParam}
import org.apache.griffin.measure.config.reader.ParamRawStringReader
import org.apache.griffin.measure.result.{DataInfo, TimeStampInfo}
import org.apache.griffin.measure.rule.expr.{Expr, StatementExpr}
import org.apache.griffin.measure.rule.{ExprValueUtil, RuleAnalyzer, RuleFactory, RuleParser}
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
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
      ("type" -> "temp"),
      ("config" -> cacheConfig)
    )

    val config = Map[String, Any](
      ("kafka.config" -> kafkaConfig),
      ("topics" -> "sss"),
      ("key.type" -> "java.lang.String"),
      ("value.type" -> "java.lang.String"),
      ("cache" -> cacheParam)
    )

    val connectorParam = DataConnectorParam("kafka", "0.8", config)

    val conf = new SparkConf().setMaster("local[*]").setAppName("ConnectorTest")
    val sc = new SparkContext(conf)
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

    ///

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

      println(s"count: ${cnt}\n ${valuestr}")

      // generate DataFrame
//      val df = genDataFrame(valueMapRdd)
//
//      // save data frame
//      cacheDataConnector.saveData(df, ms)
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true, stopGracefully=true)

    // context stop
    sc.stop

  }

  test ("rule calculation") {
//    val rules = "$source.json().name = 's2' and $source.json().age[*] = 32"
//    val rules = "$source.json().items[*] = 202 AND $source.json().age[*] = 32 AND $source.json().df[*].a = 1"
    val rules = "$source.json().items[*] = 202 AND $source.json().age[*] = 32 AND $source.json().df['a' = 1].b = 4"
//    val rules = "$source.json().df[0].a = 1"
    val ep = EvaluateRuleParam(1, rules)

    val ruleFactory = RuleFactory(ep)
    val rule: StatementExpr = ruleFactory.generateRule()
    val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

    val ruleExprs = ruleAnalyzer.sourceRuleExprs
    val constFinalExprValueMap = Map[String, Any]()

    val data = List[String](
      ("""{"name": "s1", "age": [22, 23], "items": [102, 104, 106], "df": [{"a": 1, "b": 3}, {"b": 2}]}"""),
      ("""{"name": "s2", "age": [32, 33], "items": [202, 204, 206], "df": [{"a": 1, "b": 4}, {"b": 2}]}"""),
      ("""{"name": "s3", "age": [42, 43], "items": [302, 304, 306], "df": [{"a": 1, "b": 5}, {"b": 2}]}""")
    )

    def str(expr: Expr) = {
      s"${expr._id}: ${expr.desc} [${expr.getClass.getSimpleName}]"
    }
    println("====")
    ruleExprs.finalCacheExprs.foreach { expr =>
      println(str(expr))
    }
    println("====")
    ruleExprs.cacheExprs.foreach { expr =>
      println(str(expr))
    }

    val constExprValueMap = ExprValueUtil.genExprValueMaps(None, ruleAnalyzer.constCacheExprs, Map[String, Any]())
    val finalConstExprValueMap = ExprValueUtil.updateExprValueMaps(ruleAnalyzer.constFinalCacheExprs, constExprValueMap)
    val finalConstMap = finalConstExprValueMap.headOption match {
      case Some(m) => m
      case _ => Map[String, Any]()
    }
    println("====")
    println(ruleAnalyzer.constCacheExprs)
    println(ruleAnalyzer.constFinalCacheExprs)
    println(finalConstMap)

    println("====")
    val valueMaps = data.flatMap { msg =>
      val cacheExprValueMaps = ExprValueUtil.genExprValueMaps(Some(msg), ruleExprs.cacheExprs, finalConstMap)
      val finalExprValueMaps = ExprValueUtil.updateExprValueMaps(ruleExprs.finalCacheExprs, cacheExprValueMaps)

      finalExprValueMaps
    }

    valueMaps.foreach(println)
    println(valueMaps.size)

  }

}

