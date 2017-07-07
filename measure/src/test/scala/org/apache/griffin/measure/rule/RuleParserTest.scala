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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//import org.scalatest.FlatSpec
//import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class RuleParserTest extends FunSuite with Matchers with BeforeAndAfter {

  val ruleParser = RuleParser()

  test ("literal number") {
    val rule1 = "123"
    val result1 = ruleParser.parseAll(ruleParser.literal, rule1)
    result1.successful should be (true)
    result1.get.value should be (Some(123))

    val rule2 = "12.3"
    val result2 = ruleParser.parseAll(ruleParser.literal, rule2)
    result2.successful should be (true)
    result2.get.value should be (Some(12.3))
  }

  test ("literial string") {
    val rule1 = "'123'"
    val result1 = ruleParser.parseAll(ruleParser.literal, rule1)
    result1.successful should be (true)
    result1.get.value should be (Some("123"))

    val rule2 = "\"123\""
    val result2 = ruleParser.parseAll(ruleParser.literal, rule1)
    result2.successful should be (true)
    result2.get.value should be (Some("123"))
  }

  test ("literial time") {
    val rule = "3h"
    val result = ruleParser.parseAll(ruleParser.literal, rule)
    result.successful should be (true)
    result.get.value should be (Some(3*3600*1000))
  }

  test ("literial boolean") {
    val rule = "true"
    val result = ruleParser.parseAll(ruleParser.literal, rule)
    result.successful should be (true)
    result.get.value should be (Some(true))
  }

  test ("literial null") {
    val rule = "null"
    val result = ruleParser.parseAll(ruleParser.literal, rule)
    result.successful should be (true)
    result.get.value should be (Some(null))
  }

  test ("literial none") {
    val rule = "none"
    val result = ruleParser.parseAll(ruleParser.literal, rule)
    result.successful should be (true)
    result.get.value should be (None)
  }

  test ("selection head") {
    val rule = "$source"
    val result = ruleParser.parseAll(ruleParser.selectionHead, rule)
    result.successful should be (true)
    result.get.head should be ("source")
  }

  test ("field select") {
    val rule = ".name"
    val result = ruleParser.parseAll(ruleParser.selector, rule)
    result.successful should be (true)
    result.get.desc should be ("['name']")
  }

  test ("function operation") {
    val rule = ".func(1, 'abc', 3 + 4)"
    val result = ruleParser.parseAll(ruleParser.selector, rule)
    result.successful should be (true)
    result.get.desc should be (".func(1, 'abc', 3 + 4)")
  }

  test ("index field range select") {
    val rule1 = "['field']"
    val result1 = ruleParser.parseAll(ruleParser.selector, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("['field']")

    val rule2 = "[1, 4]"
    val result2 = ruleParser.parseAll(ruleParser.selector, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("[1, 4]")

    val rule3 = "[1, 'name', 'age', 5, (6, 8)]"
    val result3 = ruleParser.parseAll(ruleParser.selector, rule3)
    result3.successful should be (true)
    result3.get.desc should be ("[1, 'name', 'age', 5, (6, 8)]")
  }

  test ("index field range") {
    val rule1 = "(3, 5)"
    val result1 = ruleParser.parseAll(ruleParser.indexFieldRange, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("(3, 5)")

    val rule2 = "'name'"
    val result2 = ruleParser.parseAll(ruleParser.indexFieldRange, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("'name'")

    val rule3 = "*"
    val result3 = ruleParser.parseAll(ruleParser.indexFieldRange, rule3)
    result3.successful should be (true)
    result3.get.desc should be ("*")
  }

  test ("filter select") {
    val rule = "['age' > 16]"
    val result = ruleParser.parseAll(ruleParser.selector, rule)
    result.successful should be (true)
    result.get.desc should be ("['age' > 16]")
  }

  test ("selection") {
    val rule = "$source['age' > 16].func(1, 'abc')[1, 3, 'name'].time[*]"
    val result = ruleParser.parseAll(ruleParser.selection, rule)
    result.successful should be (true)
    result.get.desc should be ("$source['age' > 16].func(1, 'abc')[1, 3, 'name']['time'][*]")
  }

  test ("math expr") {
    val rule = "$source.age * 6 + 4 / 2"
    val result = ruleParser.parseAll(ruleParser.mathExpr, rule)
    result.successful should be (true)
    result.get.desc should be ("$source['age'] * 6 + 4 / 2")
  }

  test ("range expr") {
    val rule = "($source.age + 1, $target.age + 3, 40)"
    val result = ruleParser.parseAll(ruleParser.rangeExpr, rule)
    result.successful should be (true)
    result.get.desc should be ("($source['age'] + 1, $target['age'] + 3, 40)")
  }

  test ("logical expr") {
    val rule1 = "$source.age + 1 = $target.age"
    val result1 = ruleParser.parseAll(ruleParser.logicalExpr, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("$source['age'] + 1 = $target['age']")

    val rule2 = "$source.age in (3, 5, 6, 10)"
    val result2 = ruleParser.parseAll(ruleParser.logicalExpr, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("$source['age'] in (3, 5, 6, 10)")
  }

  test ("logical statement") {
    val rule1 = "$source.descs[0] = $target.desc AND $source.name = $target.name"
    val result1 = ruleParser.parseAll(ruleParser.logicalStatement, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("$source['descs'][0] = $target['desc'] AND $source['name'] = $target['name']")

    val rule2 = "NOT $source.age = $target.age"
    val result2 = ruleParser.parseAll(ruleParser.logicalStatement, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("NOT $source['age'] = $target['age']")
  }

  test ("whole rule") {
    val rule1 = "$source.name = $target.name AND $source.age = $target.age"
    val result1 = ruleParser.parseAll(ruleParser.rule, rule1)
    result1.successful should be (true)
    result1.get.desc should be ("$source['name'] = $target['name'] AND $source['age'] = $target['age']")

    val rule2 = "$source.name = $target.name AND $source.age = $target.age WHEN $source.id > 1000"
    val result2 = ruleParser.parseAll(ruleParser.rule, rule2)
    result2.successful should be (true)
    result2.get.desc should be ("$source['name'] = $target['name'] AND $source['age'] = $target['age'] when $source['id'] > 1000")
  }
}
