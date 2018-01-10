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
package org.apache.griffin.measure.rule.adaptor

import org.apache.griffin.measure.process._
import org.apache.griffin.measure.process.temp.{TableRegisters, _}
import org.apache.griffin.measure.rule.plan.CalcTimeInfo
import org.apache.griffin.measure.utils.JsonUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class GriffinDslAdaptorTest extends FunSuite with Matchers with BeforeAndAfter with MockFactory {

  test ("profiling groupby") {
//    val adaptor = GriffinDslAdaptor("source" :: "target" :: Nil, "count" :: Nil)
//
//    val ruleJson =
//      """
//        |{
//        |  "dsl.type": "griffin-dsl",
//        |  "dq.type": "accuracy",
//        |  "name": "accu",
//        |  "rule": "source.user_id = target.user_id",
//        |  "details": {
//        |    "source": "source",
//        |    "target": "target",
//        |    "miss": "miss_count",
//        |    "total": "total_count",
//        |    "matched": "matched_count"
//        |  },
//        |  "metric": {
//        |    "name": "accu"
//        |  },
//        |  "record": {
//        |    "name": "missRecords"
//        |  }
//        |}
//      """.stripMargin
//
//    // rule: Map[String, Any]
//    val rule: Map[String, Any] = JsonUtil.toAnyMap(ruleJson)
//    println(rule)
//
////    val dataCheckerMock = mock[DataChecker]
////    dataCheckerMock.existDataSourceName _ expects ("source") returning (true)
////    RuleAdaptorGroup.dataChecker = dataCheckerMock
//
//    val dsTmsts = Map[String, Set[Long]](("source" -> Set[Long](1234)))
//
//    val timeInfo = CalcTimeInfo(123)
//    TableRegisters.registerCompileTempTable(timeInfo.key, "source")
//
//    val rp = adaptor.genRulePlan(timeInfo, rule, StreamingProcessType)
//    rp.ruleSteps.foreach(println)
//    rp.ruleExports.foreach(println)
  }

  test ("accuracy") {
//    val adaptor = GriffinDslAdaptor("source" :: "target" :: Nil, "count" :: Nil, StreamingProcessType, RunPhase)
//
//    val ruleJson =
//      """
//        |{
//        |  "dsl.type": "griffin-dsl",
//        |  "dq.type": "accuracy",
//        |  "name": "accu",
//        |  "rule": "source.id = target.id and source.name = target.name",
//        |  "details": {
//        |    "source": "source",
//        |    "target": "target",
//        |    "persist.type": "metric"
//        |  }
//        |}
//      """.stripMargin
//
//    // rule: Map[String, Any]
//    val rule: Map[String, Any] = JsonUtil.toAnyMap(ruleJson)
//    println(rule)
//
//    val dataCheckerMock = mock[DataChecker]
//    dataCheckerMock.existDataSourceName _ expects ("source") returns (true)
//    dataCheckerMock.existDataSourceName _ expects ("target") returns (true)
//    RuleAdaptorGroup.dataChecker = dataCheckerMock
//
//    val dsTmsts = Map[String, Set[Long]](("source" -> Set[Long](1234)), ("target" -> Set[Long](1234)))
//    val steps = adaptor.genConcreteRuleStep(TimeInfo(0, 0), rule, dsTmsts)
//
//    steps.foreach { step =>
//      println(s"${step}, ${step.ruleInfo.persistType}")
//    }
  }

  test ("uniqueness") {
//    val adaptor = GriffinDslAdaptor("new" :: "old" :: Nil, "count" :: Nil)
//    val ruleJson =
//      """
//        |{
//        |  "dsl.type": "griffin-dsl",
//        |  "dq.type": "uniqueness",
//        |  "name": "dup",
//        |  "rule": "name, count(age + 1) as ct",
//        |  "details": {
//        |    "count": "cnt"
//        |  },
//        |  "metric": {
//        |    "name": "dup"
//        |  }
//        |}
//      """.stripMargin
//    val rule: Map[String, Any] = JsonUtil.toAnyMap(ruleJson)
//    println(rule)
//
//    val timeInfo = CalcTimeInfo(123)
//    TableRegisters.registerCompileTempTable(timeInfo.key, "new")
//    TableRegisters.registerCompileTempTable(timeInfo.key, "old")
//
//    val rp = adaptor.genRulePlan(timeInfo, rule, StreamingProcessType)
//    rp.ruleSteps.foreach(println)
//    rp.ruleExports.foreach(println)
//
//    TableRegisters.unregisterCompileTempTables(timeInfo.key)
  }

  test ("timeliness") {
//    val adaptor = GriffinDslAdaptor("source" :: Nil, "length" :: Nil)
//    val ruleJson =
//      """
//        |{
//        |  "dsl.type": "griffin-dsl",
//        |  "dq.type": "timeliness",
//        |  "name": "timeliness",
//        |  "rule": "ts",
//        |  "details": {
//        |    "source": "source",
//        |    "latency": "latency",
//        |    "threshold": "1h"
//        |  },
//        |  "metric": {
//        |    "name": "timeliness"
//        |  },
//        |  "record": {
//        |    "name": "lateRecords"
//        |  }
//        |}
//      """.stripMargin
//    val rule: Map[String, Any] = JsonUtil.toAnyMap(ruleJson)
//    println(rule)
//
//    val timeInfo = CalcTimeInfo(123)
//    TableRegisters.registerCompileTempTable(timeInfo.key, "source")
//
//    val rp = adaptor.genRulePlan(timeInfo, rule, StreamingProcessType)
//    rp.ruleSteps.foreach(println)
//    rp.ruleExports.foreach(println)
//
//    TableRegisters.unregisterCompileTempTables(timeInfo.key)
  }

}
