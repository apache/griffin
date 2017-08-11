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
package org.apache.griffin.measure.rule

import org.apache.griffin.measure.config.params.user.EvaluateRuleParam
import org.apache.griffin.measure.rule.expr.{Expr, StatementExpr}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ExprValueUtilTest extends FunSuite with BeforeAndAfter with Matchers {

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
      ("""{"name": "s1", "age": [22, 23], "items": [102, 104, 106], "df": [{"a": 1, "b": 3}, {"a": 2, "b": 4}]}"""),
      ("""{"name": "s2", "age": [32, 33], "items": [202, 204, 206], "df": [{"a": 1, "b": 4}, {"a": 2, "b": 4}]}"""),
      ("""{"name": "s3", "age": [42, 43], "items": [302, 304, 306], "df": [{"a": 1, "b": 5}, {"a": 2, "b": 4}]}""")
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
