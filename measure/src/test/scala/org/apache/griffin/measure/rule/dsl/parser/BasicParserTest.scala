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
package org.apache.griffin.measure.rule.dsl.parser

import org.apache.griffin.measure.rule.dsl.expr._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//import org.scalatest.FlatSpec
//import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class BasicParserTest extends FunSuite with Matchers with BeforeAndAfter {

  val parser = new BasicParser{
    val dataSourceNames: Seq[String] = "source" :: "target" :: Nil
    val functionNames: Seq[String] = "func" :: "get_json_object" :: Nil
    def rootExpression: Parser[Expr] = expression
  }

  test("test literal") {
    val rule1 = """null"""
    val result1 = parser.parseAll(parser.literal, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("NULL")

    val rule2 = """nan"""
    val result2 = parser.parseAll(parser.literal, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("NaN")

    val rule3 = """'test\'ing'"""
    val result3 = parser.parseAll(parser.literal, rule3)
    result3.successful should be (true)
    result3.get.desc should be ("'test\\'ing'")

    val rule4 = """"test\" again""""
    val result4 = parser.parseAll(parser.literal, rule4)
    result4.successful should be (true)
    result4.get.desc should be ("\"test\\\" again\"")

    val rule5 = """-1.342"""
    val result5 = parser.parseAll(parser.literal, rule5)
    result5.successful should be (true)
    result5.get.desc should be ("-1.342")

    val rule51 = """33"""
    val result51 = parser.parseAll(parser.literal, rule51)
    result51.successful should be (true)
    result51.get.desc should be ("33")

    val rule6 = """2h"""
    val result6 = parser.parseAll(parser.literal, rule6)
    result6.successful should be (true)
    result6.get.desc should be (s"${2 * 3600 * 1000}")

    val rule7 = """true"""
    val result7 = parser.parseAll(parser.literal, rule7)
    result7.successful should be (true)
    result7.get.desc should be ("true")
  }

  test ("test selection") {
    val rule1 = """source"""
    val result1 = parser.parseAll(parser.selection, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("source")

    val rule2 = """source_not_registered"""
    val result2 = parser.parseAll(parser.selection, rule2)
    result2.successful should be (false)

    val rule3 = """source[12].age"""
    val result3 = parser.parseAll(parser.selection, rule3)
    result3.successful should be (true)
    result3.get.desc should be ("source[12].age")
    result3.get.alias should be (Some("12_age"))

    val rule4 = """source.name.func(target.name)"""
    val result4 = parser.parseAll(parser.selection, rule4)
    result4.successful should be (true)
    result4.get.desc should be ("func(source.name, target.name)")
  }

  test ("test math") {
    val rule1 = """-1"""
    val result1 = parser.parseAll(parser.mathExpression, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("(-1)")

    val rule2 = "1 + 1"
    val result2 = parser.parseAll(parser.mathExpression, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("1 + 1")

    val rule3 = "source.age + 2 * 5 + target.offset"
    val result3 = parser.parseAll(parser.mathExpression, rule3)
    result3.successful should be (true)
    result3.get.desc should be ("source.age + 2 * 5 + target.offset")

    val rule4 = "(source.age + 2) * (5 + target.offset)"
    val result4 = parser.parseAll(parser.mathExpression, rule4)
    result4.successful should be (true)
    result4.get.desc should be ("(source.age + 2) * (5 + target.offset)")
  }

  test ("test logical") {
    val rule1 = "source.age in (12 + 3, 23, 34)"
    val result1 = parser.parseAll(parser.logicalExpression, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("source.age IN (12 + 3, 23, 34)")

    val rule2 = "source.age between (12 + 3, 23, 34)"
    val result2 = parser.parseAll(parser.logicalExpression, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("source.age BETWEEN 12 + 3 AND 23")

    val rule3 = "source.age between (12 + 3)"
    assertThrows[Exception](parser.parseAll(parser.logicalExpression, rule3))

    val rule4 = "source.name like '%tk'"
    val result4 = parser.parseAll(parser.logicalExpression, rule4)
    result4.successful should be (true)
    result4.get.desc should be ("source.name LIKE '%tk'")

    val rule5 = "source.desc is not null"
    val result5 = parser.parseAll(parser.logicalExpression, rule5)
    result5.successful should be (true)
    result5.get.desc should be ("source.desc IS NOT NULL")

    val rule6 = "source.desc is not nan"
    val result6 = parser.parseAll(parser.logicalExpression, rule6)
    result6.successful should be (true)
    result6.get.desc should be ("NOT isnan(source.desc)")

    val rule7 = "!source.ok and source.name = target.name && (source.age between 12 and 52) && target.desc is not null"
    val result7 = parser.parseAll(parser.logicalExpression, rule7)
    result7.successful should be (true)
    result7.get.desc should be ("(NOT source.ok) AND source.name = target.name AND (source.age BETWEEN 12 AND 52) AND target.desc IS NOT NULL")

    val rule8 = "!(10 != 30 and !(31 > 2) or (45 <= 8 and 33 <> 0))"
    val result8 = parser.parseAll(parser.logicalExpression, rule8)
    result8.successful should be (true)
    result8.get.desc should be ("(NOT (10 != 30 AND (NOT (31 > 2)) OR (45 <= 8 AND 33 <> 0)))")

  }

  test ("test expression") {
    val rule3 = "source.age + 2 * 5 + target.offset"
    val result3 = parser.parseAll(parser.expression, rule3)
    println(result3)
    result3.successful should be (true)
    result3.get.desc should be ("source.age + 2 * 5 + target.offset")

    val rule4 = "(source.age + 2) * (5 + target.offset)"
    val result4 = parser.parseAll(parser.expression, rule4)
    println(result4)
    result4.successful should be (true)
    result4.get.desc should be ("(source.age + 2) * (5 + target.offset)")

    val rule7 = "!source.ok and source.name = target.name && (source.age between 12 and 52) && target.desc is not null"
    val result7 = parser.parseAll(parser.expression, rule7)
    result7.successful should be (true)
    result7.get.desc should be ("(NOT source.ok) AND source.name = target.name AND (source.age BETWEEN 12 AND 52) AND target.desc IS NOT NULL")

    val rule8 = "!(10 != 30 and !(31 > 2) or (45 <= 8 and 33 <> 0))"
    val result8 = parser.parseAll(parser.expression, rule8)
    result8.successful should be (true)
    result8.get.desc should be ("(NOT (10 != 30 AND (NOT (31 > 2)) OR (45 <= 8 AND 33 <> 0)))")

    val rule1 = "source.user_id = target.user_id AND source.first_name = target.first_name AND source.last_name = target.last_name AND source.address = target.address AND source.email = target.email AND source.phone = target.phone AND source.post_code = target.post_code"
    val result1 = parser.parseAll(parser.expression, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("source.user_id = target.user_id AND source.first_name = target.first_name AND source.last_name = target.last_name AND source.address = target.address AND source.email = target.email AND source.phone = target.phone AND source.post_code = target.post_code")
  }

  test ("test function") {
    val rule3 = "source.age + 2 * 5 + target.offset * func('a', source.name)"
    val result3 = parser.parseAll(parser.expression, rule3)
    result3.successful should be (true)
    result3.get.desc should be ("source.age + 2 * 5 + target.offset * func('a', source.name)")
  }

  test ("order by clause") {
    val rule = "order by source.user_id, item"
    val result = parser.parseAll(parser.orderbyClause, rule)
    result.successful should be (true)
    println(result.get.desc)
  }

  test ("select clause") {
    val rule = "select `source`.user_id, item, `source`.age.func()"
    val result = parser.parseAll(parser.selectClause, rule)
    println(result)
    result.successful should be (true)
    println(result.get.desc)
  }

  test ("from clause") {
    val rule = "from `source`"
    val result = parser.parseAll(parser.fromClause, rule)
    result.successful should be (true)
    println(result.get.desc)
  }

}
