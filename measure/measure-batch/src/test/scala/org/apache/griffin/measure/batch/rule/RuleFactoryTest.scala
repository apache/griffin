package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.config.params.user.EvaluateRuleParam
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class RuleFactoryTest extends FunSuite with BeforeAndAfter with Matchers {

  test ("generate rule") {
    val rule = "$source.name = $target.name AND $source.age = $target.age"
    val evaluateRuleParam = EvaluateRuleParam(1.0, rule)
    val ruleFactory = RuleFactory(evaluateRuleParam)
    ruleFactory.generateRule.desc should be ("$source['name'] = $target['name'] AND $source['age'] = $target['age']")

    val wrong_rule = "$source.name = $target.name AND $source.age = $target1.age"
    val evaluateRuleParam1 = EvaluateRuleParam(1.0, wrong_rule)
    val ruleFactory1 = RuleFactory(evaluateRuleParam1)
    val thrown = intercept[Exception] {
      ruleFactory1.generateRule
    }
    thrown.getMessage should be ("parse rule error!")
  }

}
