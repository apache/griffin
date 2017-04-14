package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.rule.expr._

import scala.util.{Success, Try}


case class RuleFactory(evaluateRuleParam: EvaluateRuleParam) {

  val ruleParser: RuleParser = RuleParser()

  def generateRule(): StatementExpr = {
    val rules = evaluateRuleParam.rules
    val statements = parseExpr(rules) match {
      case Success(se) => flatStatements(se)
      case _ => Nil
    }
    StatementsExpr(statements)
  }

  private def parseExpr(rules: String): Try[StatementExpr] = {
    Try {
      val result = ruleParser.parseAll(ruleParser.statementsExpr, rules)
      if (result.successful) result.get
      else throw new Exception("parse rule error!")
    }
  }

  private def flatStatements(expr: StatementExpr): Iterable[StatementExpr] = {
    expr match {
      case se: StatementsExpr => se.statements.flatMap(flatStatements(_))
      case _ => expr :: Nil
    }
  }

}
