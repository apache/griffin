package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._

import scala.util.parsing.combinator._

case class RuleParser() extends JavaTokenParsers with Serializable {

  // basic
  val anyString: Parser[String] = """[^'{}\[\]()=<>.$@;+\-*/\\]*""".r
  val variable: Parser[String] = """[a-zA-Z_]\w*""".r
  val number: Parser[String] = """[+\-]?\d+""".r
  val time: Parser[String] = """\d+(y|M|w|d|h|m|s|ms)""".r

  val numPosition: Parser[String] = """\d+""".r
  val anyPosition: Parser[String] = "*"

  val filterOpr: Parser[String] = "=" | "!=" | ">" | "<" | ">=" | "<="

  val opr1: Parser[String] = "*" | "/" | "%"
  val opr2: Parser[String] = "+" | "-"

  val assignOpr: Parser[String] = "="
  val compareOpr: Parser[String] = "==" | "!=" | ">" | "<" | ">=" | "<="
  val mappingOpr: Parser[String] = "==="

  val exprSep: Parser[String] = ";"

  // simple
  def variableString: Parser[VariableExpr] = "'" ~> anyString <~ "'" ^^ { VariableStringExpr(_) }
  def constString: Parser[ConstExpr] = "'" ~> anyString <~ "'" ^^ { ConstStringExpr(_) }
  def constValue: Parser[ConstExpr] = time ^^ { ConstTimeExpr(_) } | number ^^ { ConstNumberExpr(_)} | constString
  def variableValue: Parser[VariableExpr] = variable ^^ { VariableStringExpr(_) }
  def quoteVariableValue: Parser[QuoteVariableExpr] = "${" ~> variable <~ "}" ^^ { QuoteVariableExpr(_) }
  def position: Parser[SelectExpr] = anyPosition ^^ { AnyPositionExpr(_) } | """\d+""".r ^^ { NumPositionExpr(_) } | "'" ~> anyString <~ "'" ^^ { StringPositionExpr(_) }
  def argument: Parser[ConstExpr] = constValue
  def annotationExpr: Parser[AnnotationExpr] = "@" ~> variable ^^ { AnnotationExpr(_) }

  // selector
  def filterOpration: Parser[SelectExpr] = (variableString ~ filterOpr ~ constString) ^^ {
    case v ~ opr ~ c => FilterOprExpr(opr, v, c)
  }
  def positionExpr: Parser[SelectExpr] = "[" ~> (filterOpration | position) <~ "]"
  def functionExpr: Parser[SelectExpr] = "." ~ variable ~ "(" ~ repsep(argument, ",") ~ ")" ^^ {
    case _ ~ v ~ _ ~ args ~ _ => FunctionExpr(v, args)
  }
  def selectorExpr: Parser[SelectExpr] = positionExpr | functionExpr

  // data
  def selectorsExpr: Parser[DataExpr] = quoteVariableValue ~ rep(selectorExpr) ^^ {
    case q ~ tails => SelectionExpr(q, tails)
  }

  // calculation
  def factor: Parser[ElementExpr] = (constValue | selectorsExpr | "(" ~> expr <~ ")") ^^ { FactorExpr(_) }
  def term: Parser[ElementExpr] = factor ~ rep(opr1 ~ factor) ^^ {
    case a ~ Nil => a
    case a ~ list => CalculationExpr(a, list.map(c => (c._1, c._2)))
  }
  def expr: Parser[ElementExpr] = term ~ rep(opr2 ~ term) ^^ {
    case a ~ Nil => a
    case a ~ list => CalculationExpr(a, list.map(c => (c._1, c._2)))
  }

  // statement
  def assignExpr: Parser[StatementExpr] = variableValue ~ assignOpr ~ expr ^^ {
    case v ~ opr ~ c => AssignExpr(opr, v, c)
  }
  def conditionExpr: Parser[StatementExpr] = rep(annotationExpr) ~ expr ~ compareOpr ~ expr ^^ {
    case anos ~ le ~ opr ~ re => ConditionExpr(opr, le, re, anos)
  }
  def mappingExpr: Parser[StatementExpr] = rep(annotationExpr) ~ expr ~ mappingOpr ~ expr ^^ {
    case anos ~ le ~ opr ~ re => MappingExpr(opr, le, re, anos)
  }
  def statementExpr: Parser[StatementExpr] = assignExpr | conditionExpr | mappingExpr

  // statements
  def statementsExpr: Parser[StatementExpr] = repsep(statementExpr, exprSep) ^^ { StatementsExpr(_) }

}
