/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.rule

import org.apache.griffin.measure.config.params.user.EvaluateRuleParam
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
