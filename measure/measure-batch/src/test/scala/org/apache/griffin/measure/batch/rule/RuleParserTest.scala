package org.apache.griffin.measure.batch.rule

//import org.apache.griffin.measure.batch.log.Loggable
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//
//@RunWith(classOf[JUnitRunner])
//class RuleParserTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {
//
////  test("test rule parser") {
////    val ruleParser = RuleParser()
////
//////    val rules = "outTime = 24h; @Invalid ${source}['__time'] + ${outTime} > ${target}['__time']"
////    val rules = "@Key ${source}.json()['seeds'][*].json()['metadata'].json()['tracker']['crawlRequestCreateTS'] === ${target}.json()['groups'][0][\"attrsList\"]['name'=\"CRAWLMETADATA\"]['values'][0].json()['tracker']['crawlRequestCreateTS']"
//////    val rules = "${source}['__time'] + ${outTime} > ${target}['__time']"
//////    val rules = "${source}['__time'] > ${target}['__time']"
//////    val rules = "432"
//////    val rules = "${target}.json()['groups'][0]['attrsList']['name'='URL']['values'][0]"
////
////    val result = ruleParser.parseAll(ruleParser.statementsExpr, rules)
////
////    println(result)
////  }
//
////  test("treat escape") {
////    val es = """Hello\tworld\nmy name is \"ABC\""""
////    val un = StringContext treatEscapes es
////
////    println(es)
////    println(un)
////  }
//
//  test("test rule parser") {
//    val ruleParser = RuleParser()
//
////    val rules = "$SOUrce['tgt' < $source['tag' != 2] - -+-++---1] between ( -$target['32a'] + 9, 100, ----1000 ) and (45 > 9 or $target.type + 8 == 9 and $source['a'] >= 0) when not not not not $source._time + 24h < $target._time"
//    val rules = "$source['aaaf fd', (21, 43), '12']"
//
//    val result = ruleParser.parseAll(ruleParser.selection, rules)
//
//    println(result)
//  }
//
//  test("test rule analyzer") {
//    val ruleParser = RuleParser()
//
////    val rules = "$source.tag == $target['take' >= 5] and $source.price + $source.price1 > $target['kk' < $target.age] and $source.ee = $target.fe + $target.a when $target.ggg = 1"
//    val rules = "$source.tag = $target.tag WHEN true"
//    val result = ruleParser.parseAll(ruleParser.rule, rules)
//    println(result)
//
//    if (result.successful) {
//      val ruleAnalyzer = RuleAnalyzer(result.get)
//
//      println("source")
//      println(ruleAnalyzer.sourceRuleExprs)
//      println("source")
//      println(ruleAnalyzer.targetRuleExprs)
//    }
//
//
//
//  }
//
//}

import org.apache.griffin.measure.batch.rule.expr._
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

}
