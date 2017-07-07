/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.batch.rule

import org.apache.griffin.measure.batch.rule.expr._

import scala.util.parsing.combinator._

case class RuleParser() extends JavaTokenParsers with Serializable {

  /**
    * BNF representation for grammar as below:
    *
    * <rule> ::= <logical-statement> [WHEN <logical-statement>]
    * rule: mapping-rule [WHEN when-rule]
    * - mapping-rule: the first level opr should better not be OR | NOT, otherwise it can't automatically find the groupby column
    * - when-rule: only contain the general info of data source, not the special info of each data row
    *
    * <logical-statement> ::= [NOT] <logical-expression> [(AND | OR) <logical-expression>]+ | "(" <logical-statement> ")"
    * logical-statement: return boolean value
    * logical-operator: "AND" | "&&", "OR" | "||", "NOT" | "!"
    *
    * <logical-expression> ::= <math-expr> (<compare-opr> <math-expr> | <range-opr> <range-expr>)
    * logical-expression example: $source.id = $target.id, $source.page_id IN ('3214', '4312', '60821')
    *
    * <compare-opr> ::= "=" | "!=" | "<" | ">" | "<=" | ">="
    * <range-opr> ::= ["NOT"] "IN" | "BETWEEN"
    * <range-expr> ::= "(" [<math-expr>] [, <math-expr>]+ ")"
    * range-expr example: ('3214', '4312', '60821'), (10, 15), ()
    *
    * <math-expr> ::= [<unary-opr>] <math-factor> [<binary-opr> <math-factor>]+
    * math-expr example: $source.price * $target.count, "hello" + " " + "world" + 123
    *
    * <binary-opr> ::= "+" | "-" | "*" | "/" | "%"
    * <unary-opr> ::= "+" | "-"
    *
    * <math-factor> ::= <literal> | <selection> | "(" <math-expr> ")"
    *
    * <selection> ::= <selection-head> [ <field-sel> | <function-operation> | <index-field-range-sel> | <filter-sel> ]+
    * selection example: $source.price, $source.json(), $source['state'], $source.numList[3], $target.json().mails['org' = 'apache'].names[*]
    *
    * <selection-head> ::= $source | $target
    *
    * <field-sel> ::= "." <field-string>
    *
    * <function-operation> ::= "." <function-name> "(" <arg> [, <arg>]+ ")"
    * <function-name> ::= <name-string>
    * <arg> ::= <math-expr>
    *
    * <index-field-range-sel> ::= "[" <index-field-range> [, <index-field-range>]+ "]"
    * <index-field-range> ::= <index-field> | (<index-field>, <index-field>) | "*"
    * index-field-range: 2 means the 3rd item, (0, 3) means first 4 items, * means all items, 'age' means item 'age'
    * <index-field> ::= <index> | <field-quote> | <all-selection>
    * index: 0 ~ n means position from start, -1 ~ -n means position from end
    * <field-quote> ::= ' <field-string> ' | " <field-string> "
    *
    * <filter-sel> ::= "[" <field-quote> <filter-compare-opr> <math-expr> "]"
    * <filter-compare-opr> ::= "=" | "!=" | "<" | ">" | "<=" | ">="
    * filter-sel example: ['name' = 'URL'], $source.man['age' > $source.graduate_age + 5 ]
    *
    * When <math-expr> in the selection, it mustn't contain the different <selection-head>, for example:
    * $source.tags[1+2]             valid
    * $source.tags[$source.first]   valid
    * $source.tags[$target.first]   invalid
    * -- Such job is for validation, not for parser
    *
    *
    * <literal> ::= <literal-string> | <literal-number> | <literal-time> | <literal-boolean> | <literal-null> | <literal-none>
    * <literal-string> ::= <any-string>
    * <literal-number> ::= <integer> | <double>
    * <literal-time> ::= <integer> ("d"|"h"|"m"|"s"|"ms")
    * <literal-boolean> ::= true | false
    * <literal-null> ::= null | undefined
    * <literal-none> ::= none
    *
    */

  object Keyword {
    def WhenKeywords: Parser[String] = """(?i)when""".r
    def UnaryLogicalKeywords: Parser[String] = """(?i)not""".r
    def BinaryLogicalKeywords: Parser[String] = """(?i)and|or""".r
    def RangeKeywords: Parser[String] = """(?i)(not\s+)?(in|between)""".r
    def DataSourceKeywords: Parser[String] = """(?i)\$(source|target)""".r
    def Keywords: Parser[String] = WhenKeywords | UnaryLogicalKeywords | BinaryLogicalKeywords | RangeKeywords | DataSourceKeywords
  }
  import Keyword._

  object Operator {
    def NotLogicalOpr: Parser[String] = """(?i)not""".r | "!"
    def AndLogicalOpr: Parser[String] = """(?i)and""".r | "&&"
    def OrLogicalOpr: Parser[String] = """(?i)or""".r | "||"
    def CompareOpr: Parser[String] = """!?==?""".r | """<=?""".r | """>=?""".r
    def RangeOpr: Parser[String] = RangeKeywords

    def UnaryMathOpr: Parser[String] = "+" | "-"
    def BinaryMathOpr1: Parser[String] = "*" | "/" | "%"
    def BinaryMathOpr2: Parser[String] = "+" | "-"

    def FilterCompareOpr: Parser[String] = """!?==?""".r | """<=?""".r | """>=?""".r

    def SqBracketPair: (Parser[String], Parser[String]) = ("[", "]")
    def BracketPair: (Parser[String], Parser[String]) = ("(", ")")
    def Dot: Parser[String] = "."
    def AllSelection: Parser[String] = "*"
    def SQuote: Parser[String] = "'"
    def DQuote: Parser[String] = "\""
    def Comma: Parser[String] = ","
  }
  import Operator._

  object SomeString {
    def AnyString: Parser[String] = """[^'\"{}\[\]()=<>.$@,;+\-*/\\]*""".r
    def SimpleFieldString: Parser[String] = """\w+""".r
    def FieldString: Parser[String] = """[\w\s]+""".r
    def NameString: Parser[String] = """[a-zA-Z_]\w*""".r
  }
  import SomeString._

  object SomeNumber {
    def IntegerNumber: Parser[String] = """[+\-]?\d+""".r
    def DoubleNumber: Parser[String] = """[+\-]?(\.\d+|\d+\.\d*)""".r
    def IndexNumber: Parser[String] = IntegerNumber
  }
  import SomeNumber._

  // -- literal --
  def literal: Parser[LiteralExpr] = literialString | literialTime | literialNumber | literialBoolean | literialNull | literialNone
  def literialString: Parser[LiteralStringExpr] = (SQuote ~> AnyString <~ SQuote | DQuote ~> AnyString <~ DQuote) ^^ { LiteralStringExpr(_) }
  def literialNumber: Parser[LiteralNumberExpr] = (DoubleNumber | IntegerNumber) ^^ { LiteralNumberExpr(_) }
  def literialTime: Parser[LiteralTimeExpr] = """(\d+(d|h|m|s|ms))+""".r ^^ { LiteralTimeExpr(_) }
  def literialBoolean: Parser[LiteralBooleanExpr] = ("""(?i)true""".r | """(?i)false""".r) ^^ { LiteralBooleanExpr(_) }
  def literialNull: Parser[LiteralNullExpr] = ("""(?i)null""".r | """(?i)undefined""".r) ^^ { LiteralNullExpr(_) }
  def literialNone: Parser[LiteralNoneExpr] = """(?i)none""".r ^^ { LiteralNoneExpr(_) }

  // -- selection --
  // <selection> ::= <selection-head> [ <field-sel> | <function-operation> | <index-field-range-sel> | <filter-sel> ]+
  def selection: Parser[SelectionExpr] = selectionHead ~ rep(selector) ^^ {
    case head ~ selectors => SelectionExpr(head, selectors)
  }
  def selector: Parser[SelectExpr] = (functionOperation | fieldSelect | indexFieldRangeSelect | filterSelect)

  def selectionHead: Parser[SelectionHead] = DataSourceKeywords ^^ { SelectionHead(_) }
  // <field-sel> ::= "." <field-string>
  def fieldSelect: Parser[IndexFieldRangeSelectExpr] = Dot ~> SimpleFieldString ^^ {
    case field => IndexFieldRangeSelectExpr(FieldDesc(field) :: Nil)
  }
  // <function-operation> ::= "." <function-name> "(" <arg> [, <arg>]+ ")"
  def functionOperation: Parser[FunctionOperationExpr] = Dot ~ NameString ~ BracketPair._1 ~ repsep(argument, Comma) ~ BracketPair._2 ^^ {
    case _ ~ func ~ _ ~ args ~ _ => FunctionOperationExpr(func, args)
  }
  def argument: Parser[MathExpr] = mathExpr
  // <index-field-range-sel> ::= "[" <index-field-range> [, <index-field-range>]+ "]"
  def indexFieldRangeSelect: Parser[IndexFieldRangeSelectExpr] = SqBracketPair._1 ~> rep1sep(indexFieldRange, Comma) <~ SqBracketPair._2 ^^ {
    case ifrs => IndexFieldRangeSelectExpr(ifrs)
  }
  // <index-field-range> ::= <index-field> | (<index-field>, <index-field>) | "*"
  def indexFieldRange: Parser[FieldDescOnly] = indexField | BracketPair._1 ~ indexField ~ Comma ~ indexField ~ BracketPair._2 ^^ {
    case _ ~ if1 ~ _ ~ if2 ~ _ => FieldRangeDesc(if1, if2)
  }
  // <index-field> ::= <index> | <field-quote> | <all-selection>
  // *here it can parse <math-expr>, but for simple situation, not supported now*
  def indexField: Parser[FieldDescOnly] = IndexNumber ^^ { IndexDesc(_) } | fieldQuote | AllSelection ^^ { AllFieldsDesc(_) }
  // <field-quote> ::= ' <field-string> ' | " <field-string> "
  def fieldQuote: Parser[FieldDesc] = (SQuote ~> FieldString <~ SQuote | DQuote ~> FieldString <~ DQuote) ^^ { FieldDesc(_) }
  // <filter-sel> ::= "[" <field-quote> <filter-compare-opr> <math-expr> "]"
  def filterSelect: Parser[FilterSelectExpr] = SqBracketPair._1 ~> fieldQuote ~ FilterCompareOpr ~ mathExpr <~ SqBracketPair._2 ^^ {
    case field ~ compare ~ value => FilterSelectExpr(field, compare, value)
  }

  // -- math --
  // <math-factor> ::= <literal> | <selection> | "(" <math-expr> ")"
  def mathFactor: Parser[MathExpr] = (literal | selection | BracketPair._1 ~> mathExpr <~ BracketPair._2) ^^ { MathFactorExpr(_) }
  // <math-expr> ::= [<unary-opr>] <math-factor> [<binary-opr> <math-factor>]+
  // <unary-opr> ::= "+" | "-"
  def unaryMathExpr: Parser[MathExpr] = rep(UnaryMathOpr) ~ mathFactor ^^ {
    case Nil ~ a => a
    case list ~ a => UnaryMathExpr(list, a)
  }
  // <binary-opr> ::= "+" | "-" | "*" | "/" | "%"
  def binaryMathExpr1: Parser[MathExpr] = unaryMathExpr ~ rep(BinaryMathOpr1 ~ unaryMathExpr) ^^ {
    case a ~ Nil => a
    case a ~ list => BinaryMathExpr(a, list.map(c => (c._1, c._2)))
  }
  def binaryMathExpr2: Parser[MathExpr] = binaryMathExpr1 ~ rep(BinaryMathOpr2 ~ binaryMathExpr1) ^^ {
    case a ~ Nil => a
    case a ~ list => BinaryMathExpr(a, list.map(c => (c._1, c._2)))
  }
  def mathExpr: Parser[MathExpr] = binaryMathExpr2

  // -- logical expression --
  // <range-expr> ::= "(" [<math-expr>] [, <math-expr>]+ ")"
  def rangeExpr: Parser[RangeDesc] = BracketPair._1 ~> repsep(mathExpr, Comma) <~ BracketPair._2 ^^ { RangeDesc(_) }
  // <logical-expression> ::= <math-expr> (<compare-opr> <math-expr> | <range-opr> <range-expr>)
  def logicalExpr: Parser[LogicalExpr] = mathExpr ~ CompareOpr ~ mathExpr ^^ {
    case left ~ opr ~ right => LogicalCompareExpr(left, opr, right)
  } | mathExpr ~ RangeOpr ~ rangeExpr ^^ {
    case left ~ opr ~ range => LogicalRangeExpr(left, opr, range)
  } | mathExpr ^^ { LogicalSimpleExpr(_) }

  // -- logical statement --
  def logicalFactor: Parser[LogicalExpr] = logicalExpr | BracketPair._1 ~> logicalStatement <~ BracketPair._2
  def notLogicalStatement: Parser[LogicalExpr] = rep(NotLogicalOpr) ~ logicalFactor ^^ {
    case Nil ~ a => a
    case list ~ a => UnaryLogicalExpr(list, a)
  }
  def andLogicalStatement: Parser[LogicalExpr] = notLogicalStatement ~ rep(AndLogicalOpr ~ notLogicalStatement) ^^ {
    case a ~ Nil => a
    case a ~ list => BinaryLogicalExpr(a, list.map(c => (c._1, c._2)))
  }
  def orLogicalStatement: Parser[LogicalExpr] = andLogicalStatement ~ rep(OrLogicalOpr ~ andLogicalStatement) ^^ {
    case a ~ Nil => a
    case a ~ list => BinaryLogicalExpr(a, list.map(c => (c._1, c._2)))
  }
  // <logical-statement> ::= [NOT] <logical-expression> [(AND | OR) <logical-expression>]+ | "(" <logical-statement> ")"
  def logicalStatement: Parser[LogicalExpr] = orLogicalStatement

  // -- rule --
  // <rule> ::= <logical-statement> [WHEN <logical-statement>]
  def rule: Parser[StatementExpr] = logicalStatement ~ opt(WhenKeywords ~> logicalStatement) ^^ {
    case ls ~ Some(ws) => WhenClauseStatementExpr(ls, ws)
    case ls ~ _ => SimpleStatementExpr(ls)
  }

}
