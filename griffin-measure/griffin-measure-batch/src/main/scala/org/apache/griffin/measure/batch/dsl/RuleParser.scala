package org.apache.griffin.measure.batch.dsl

import org.apache.griffin.measure.batch.dsl.expr._

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
  def variableString: Parser[Expr] = "'" ~> anyString <~ "'" ^^ { VariableStringExpr(_) }
  def constString: Parser[Expr] = "'" ~> anyString <~ "'" ^^ { ConstStringExpr(_) }
  def constValue: Parser[Expr] = time ^^ { ConstTimeExpr(_) } | number ^^ { ConstNumberExpr(_)} | constString
  def variableValue: Parser[Expr] = variable ^^ { VariableStringExpr(_) }
  def quoteVariableValue: Parser[Expr] = "${" ~> variable <~ "}" ^^ { QuoteVariableStringExpr(_) }
  def position: Parser[Expr] = anyPosition ^^ { AnyPositionExpr(_) } | """\d+""".r ^^ { NumPositionExpr(_) } | "'" ~> anyString <~ "'" ^^ { StringPositionExpr(_) }
  def argument: Parser[Expr] = constValue
  def annotationExpr: Parser[Expr] = "@" ~> variable ^^ { AnnotationExpr(_) }

  // selector
  def filterOpration: Parser[Expr] = (variableString ~ filterOpr ~ constString) ^^ {
    case v ~ opr ~ c => FilterOprExpr(opr, v, c)
  }
  def positionExpr: Parser[Expr] = "[" ~> (filterOpration | position) <~ "]"
  def functionExpr: Parser[Expr] = "." ~ variable ~ "(" ~ repsep(argument, ",") ~ ")" ^^ {
    case _ ~ v ~ _ ~ args ~ _ => FunctionExpr(v, args)
  }
  def selectorExpr: Parser[Expr] = positionExpr | functionExpr
  def selectorsExpr: Parser[Expr] = quoteVariableValue ~ rep(selectorExpr) ^^ {
    case q ~ tails => SelectorsExpr(q, tails)
  }

  // calculation
  def factor: Parser[Expr] = constValue | selectorsExpr | "(" ~> expr <~ ")"
  def term: Parser[Expr] = factor ~ rep(opr1 ~ factor) ^^ {
    case a ~ Nil => a
    case a ~ list => WholeExpr(a, list.map(c => (c._1, c._2)))
  }
  def expr: Parser[Expr] = term ~ rep(opr2 ~ term) ^^ {
    case a ~ Nil => a
    case a ~ list => WholeExpr(a, list.map(c => (c._1, c._2)))
  }

  // statement
  def assignExpr: Parser[Expr] = variableValue ~ assignOpr ~ expr ^^ {
    case v ~ opr ~ c => AssignExpr(opr, v, c)
  }
  def conditionExpr: Parser[Expr] = rep(annotationExpr) ~ expr ~ compareOpr ~ expr ^^ {
    case anos ~ le ~ opr ~ re => ConditionExpr(opr, le, re, anos)
  }
  def mappingExpr: Parser[Expr] = rep(annotationExpr) ~ expr ~ mappingOpr ~ expr ^^ {
    case anos ~ le ~ opr ~ re => MappingExpr(opr, le, re, anos)
  }
  def statementExpr: Parser[Expr] = assignExpr | conditionExpr | mappingExpr

  // statements
  def statementsExpr: Parser[Expr] = repsep(statementExpr, exprSep) ^^ { StatementsExpr(_) }

}
