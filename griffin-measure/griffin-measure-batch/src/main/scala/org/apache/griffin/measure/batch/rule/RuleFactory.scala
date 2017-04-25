package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.config.params.user._

import scala.util.Failure
//import org.apache.griffin.measure.batch.rule.expr_old._
import org.apache.griffin.measure.batch.rule.expr._

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
