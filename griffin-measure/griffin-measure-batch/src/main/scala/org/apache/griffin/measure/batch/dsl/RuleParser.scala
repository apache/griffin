package org.apache.griffin.measure.batch.dsl

import org.apache.griffin.measure.batch.dsl.expr._

import scala.util.parsing.combinator._

case class RuleParser() extends JavaTokenParsers with Serializable {

//  def expr: Parser[Any] = term~rep("+"~term | "-"~term)
//  def term: Parser[Any] = factor~rep("*"~factor | "/"~factor)
//  def factor: Parser[Any] = floatingPointNumber | "("~expr~")"

}
