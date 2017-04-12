package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.rule.expr._

import scala.util.{Success, Try}


case class RuleFactory(assertionParam: AssertionParam) {

  object DSL {
    val DSL_Griffin = "DSL-griffin"
  }

  import DSL._

  val ruleParser: RuleParser = assertionParam.dslType match {
    case DSL_Griffin => RuleParser()
    case _ => RuleParser()
  }

  def generateRule(): StatementExpr = {
    val statements = assertionParam.rules.flatMap { ruleParam =>
      parseExpr(ruleParam) match {
        case Success(se) => flatStatements(se)
        case _ => Nil
      }
    }
    StatementsExpr(statements)
  }

  private def parseExpr(ruleParam: RuleParam): Try[StatementExpr] = {
    Try {
      val result = ruleParser.parseAll(ruleParser.statementsExpr, ruleParam.rule)
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
