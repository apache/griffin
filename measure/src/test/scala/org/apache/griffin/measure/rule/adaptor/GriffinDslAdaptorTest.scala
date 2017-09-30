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

import org.apache.griffin.measure.process.check.DataChecker
import org.apache.griffin.measure.utils.JsonUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class GriffinDslAdaptorTest extends FunSuite with Matchers with BeforeAndAfter with MockFactory {

  test ("profiling groupby") {
    val adaptor = GriffinDslAdaptor("source" :: Nil, "count" :: Nil, RunPhase)

    val ruleJson =
      """
        |{
        |  "dsl.type": "griffin-dsl",
        |  "dq.type": "profiling",
        |  "rule": "source.age, (source.user_id.COUNT() + 1s) as cnt group by source.age having source.desc.count() > 5 or false order by user_id desc, user_name asc limit 5",
        |  "details": {
        |    "source": "source",
        |    "profiling": {
        |      "name": "prof",
        |      "persist.type": "metric"
        |    }
        |  }
        |}
      """.stripMargin

    // rule: Map[String, Any]
    val rule: Map[String, Any] = JsonUtil.toAnyMap(ruleJson)
    println(rule)

    val dataCheckerMock = mock[DataChecker]
    dataCheckerMock.existDataSourceName _ expects ("source") returning (true)
    RuleAdaptorGroup.dataChecker = dataCheckerMock

    val steps = adaptor.genConcreteRuleStep(rule)

    steps.foreach { step =>
      println(s"${step.name} [${step.dslType}]: ${step.rule}")
    }
  }

}
