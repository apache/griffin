package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.config.params.user.EvaluateRuleParam
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class RuleAnalyzerTest extends FunSuite with BeforeAndAfter with Matchers {

  test ("rule analyze") {
    val rule = "$source.name = $target.name AND $source.age = $target.age + (2 * 5) WHEN $source.born > (6 - 2 * 2)"
    val evaluateRuleParam = EvaluateRuleParam(1.0, rule)
    val ruleFactory = RuleFactory(evaluateRuleParam)
    val statement = ruleFactory.generateRule

    val ruleAnalyzer = RuleAnalyzer(statement)

    ruleAnalyzer.constCacheExprs.map(_.desc) should be (List[String]("2 * 5", "2 * 2", "6 - 2 * 2"))
    ruleAnalyzer.constFinalCacheExprs.map(_.desc) should be (Set[String]("2 * 5", "6 - 2 * 2"))

    ruleAnalyzer.sourceRuleExprs.groupbyExprs.map(_.desc) should be (List[String](
      "$source['name']", "$source['age']"))
    ruleAnalyzer.sourceRuleExprs.cacheExprs.map(_.desc) should be (List[String](
      "$source['name']", "$source['age']", "$source['born']", "$source['born'] > 6 - 2 * 2"))
    ruleAnalyzer.sourceRuleExprs.finalCacheExprs.map(_.desc) should be (Set[String](
      "$source['name']", "$source['age']", "$source['born']", "$source['born'] > 6 - 2 * 2"))
    ruleAnalyzer.sourceRuleExprs.persistExprs.map(_.desc) should be (List[String](
      "$source['name']", "$source['age']", "$source['born']"))
    ruleAnalyzer.sourceRuleExprs.whenClauseExprOpt.map(_.desc) should be (Some(
      "$source['born'] > 6 - 2 * 2"))

    ruleAnalyzer.targetRuleExprs.groupbyExprs.map(_.desc) should be (List[String](
      "$target['name']", "$target['age'] + 2 * 5"))
    ruleAnalyzer.targetRuleExprs.cacheExprs.map(_.desc) should be (List[String](
      "$target['name']", "$target['age']", "$target['age'] + 2 * 5"))
    ruleAnalyzer.targetRuleExprs.finalCacheExprs.map(_.desc) should be (Set[String](
      "$target['name']", "$target['age']", "$target['age'] + 2 * 5"))
    ruleAnalyzer.targetRuleExprs.persistExprs.map(_.desc) should be (List[String](
      "$target['name']", "$target['age']"))
    ruleAnalyzer.targetRuleExprs.whenClauseExprOpt.map(_.desc) should be (Some(
      "$source['born'] > 6 - 2 * 2"))

  }

}
