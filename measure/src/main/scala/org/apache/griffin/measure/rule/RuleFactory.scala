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

import org.apache.griffin.measure.config.params.user._

import scala.util.Failure
//import org.apache.griffin.measure.rule.expr_old._
import org.apache.griffin.measure.rule.expr._

import scala.util.{Success, Try}


case class RuleFactory(evaluateRuleParam: EvaluateRuleParam) {

  val ruleParser: RuleParser = RuleParser()

  def generateRule(): StatementExpr = {
    val rules = evaluateRuleParam.rules
    val statement = parseExpr(rules) match {
      case Success(se) => se
      case Failure(ex) => throw ex
    }
    statement
  }

  private def parseExpr(rules: String): Try[StatementExpr] = {
    Try {
      val result = ruleParser.parseAll(ruleParser.rule, rules)
      if (result.successful) result.get
      else throw new Exception("parse rule error!")
//      throw new Exception("parse rule error!")
    }
  }

}
