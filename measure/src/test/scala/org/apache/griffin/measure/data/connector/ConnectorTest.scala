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
package org.apache.griffin.measure.data.connector

import java.util.Date
import java.util.concurrent.TimeUnit

import kafka.serializer.StringDecoder
import org.apache.griffin.measure.cache.info.InfoCacheInstance
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user.{DataConnectorParam, EvaluateRuleParam}
import org.apache.griffin.measure.config.reader.ParamRawStringReader
import org.apache.griffin.measure.data.connector.batch.TextDirBatchDataConnector
import org.apache.griffin.measure.process.TimingProcess
import org.apache.griffin.measure.process.engine.DqEngines
import org.apache.griffin.measure.result.{DataInfo, TimeStampInfo}
import org.apache.griffin.measure.rule._
import org.apache.griffin.measure.utils.{HdfsFileDumpUtil, HdfsUtil, TimeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
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

  test ("test sql") {
    val concreteTableName = "default.demo_src"
    val partitionsString = " dt=1, hr=2; dt=1 ; ;,hr=4; dt=2 ;;dt=5"
    val partitions: Array[Array[String]] = partitionsString.split(";").flatMap { s =>
      val arr = s.trim.split(",").flatMap { t =>
        t.trim match {
          case p if (p.nonEmpty) => Some(p)
          case _ => None
        }
      }
      if (arr.size > 0) Some(arr) else None
    }

    val tableClause = s"SELECT * FROM ${concreteTableName}"
    val validPartitions = partitions.filter(_.size > 0)
    val ret = if (validPartitions.size > 0) {
      val clauses = validPartitions.map { prtn =>
        val cls = prtn.mkString(" AND ")
        s"${tableClause} WHERE ${cls}"
      }
      clauses.mkString(" UNION ALL ")
    } else tableClause

    println(ret)
  }
}

