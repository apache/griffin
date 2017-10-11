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
package org.apache.griffin.measure.rule.dsl.parser

import org.apache.griffin.measure.rule.dsl.expr._

import scala.util.parsing.combinator.JavaTokenParsers

trait BasicParser extends JavaTokenParsers with Serializable {

  val dataSourceNames: Seq[String]
  val functionNames: Seq[String]

  private def trim(str: String): String = {
    val regex = """`(.*)`""".r
    str match {
      case regex(s) => s
      case _ => str
    }
  }

  /**
    * BNF for basic parser
    *
    * -- literal --
    * <literal> ::= <literal-string> | <literal-number> | <literal-time> | <literal-boolean> | <literal-null> | <literal-nan>
    * <literal-string> ::= <any-string>
    * <literal-number> ::= <integer> | <double>
    * <literal-time> ::= <integer> ("d"|"h"|"m"|"s"|"ms")
    * <literal-boolean> ::= true | false
    * <literal-null> ::= null
    * <literal-nan> ::= nan
    *
    * -- selection --
    * <selection> ::= <selection-head> [ <field-sel> | <index-sel> | <function-sel> ]*
    * <selection-head> ::= ("data source name registered") | <function>
    * <field-sel> ::= "." <field-name> | "[" <quote-field-name> "]"
    * <index-sel> ::= "[" <arg> "]"
    * <function-sel> ::= "." <function-name> "(" [<arg>]? [, <arg>]* ")"
    * <arg> ::= <math-expr>
    *
    * -- math expr --
    * <math-factor> ::= <literal> | <alias-expr> | <function> | <selection> | "(" <math-expr> ")"
    * <unary-math-expr> ::= [<unary-opr>]* <math-factor>
    * <binary-math-expr> ::= <unary-math-expr> [<binary-opr> <unary-math-expr>]+
    * <math-expr> ::= <binary-math-expr>
    *
    * -- logical expr --
    * <in-expr> ::= <math-expr> [<not>]? <in> <range-expr>
    * <between-expr> ::= <math-expr> [<not>]? <between> (<math-expr> <and> <math-expr> | <range-expr>)
    * <range-expr> ::= "(" [<math-expr>]? [, <math-expr>]+ ")"
    * <like-expr> ::= <math-expr> [<not>]? <like> <math-expr>
    * <is-null-expr> ::= <math-expr> <is> [<not>]? <null>
    * <is-nan-expr> ::= <math-expr> <is> [<not>]? <nan>
    *
    * <logical-factor> ::= <math-expr> | <in-expr> | <between-expr> | <like-expr> | <is-null-expr> | <is-nan-expr> | "(" <logical-expr> ")"
    * <unary-logical-expr> ::= [<unary-logical-opr>]* <logical-factor>
    * <binary-logical-expr> ::= <unary-logical-expr> [<binary-logical-opr> <unary-logical-expr>]+
    * <logical-expr> ::= <binary-logical-expr>
    *
    * -- expression --
    * <expr> = <math-expr> | <logical-expr>
    *
    * -- function expr --
    * <function> ::= <function-name> "(" [<arg>] [, <arg>]+ ")"
    * <function-name> ::= ("function name registered")
    * <arg> ::= <expr>
    *
    * -- alias expr --
    * <alias-expr> = <expr> <as> <name>
    */

  protected def genNamesParser(names: Seq[String]): Parser[String] = {
    names.reverse.map {
      fn => s"""(?i)${fn}""".r: Parser[String]
    }.reduce(_ | _)
  }

  object Literal {
    val NULL: Parser[String] = """(?i)null""".r
    val NAN: Parser[String] = """(?i)nan""".r
  }
  import Literal._

  object Operator {
    val MATH_UNARY: Parser[String] = "+" | "-"
    val MATH_BINARIES: Seq[Parser[String]] = Seq(("*" | "/" | "%"), ("+" | "-"))

    val NOT: Parser[String] = """(?i)not\s""".r | "!"
    val AND: Parser[String] = """(?i)and\s""".r | "&&"
    val OR: Parser[String] = """(?i)or\s""".r | "||"
    val IN: Parser[String] = """(?i)in\s""".r
    val BETWEEN: Parser[String] = """(?i)between\s""".r
    val AND_ONLY: Parser[String] = """(?i)and\s""".r
    val IS: Parser[String] = """(?i)is\s""".r
    val LIKE: Parser[String] = """(?i)like\s""".r
    val COMPARE: Parser[String] = "=" | "!=" | "<>" | "<=" | ">=" | "<" | ">"
    val LOGICAL_UNARY: Parser[String] = NOT
    val LOGICAL_BINARIES: Seq[Parser[String]] = Seq((COMPARE), (AND), (OR))

    val LSQBR: Parser[String] = "["
    val RSQBR: Parser[String] = "]"
    val LBR: Parser[String] = "("
    val RBR: Parser[String] = ")"

    val DOT: Parser[String] = "."
    val ALLSL: Parser[String] = "*"
    val SQUOTE: Parser[String] = "'"
    val DQUOTE: Parser[String] = "\""
    val UQUOTE: Parser[String] = "`"
    val COMMA: Parser[String] = ","

    val SELECT: Parser[String] = """(?i)select\s""".r
    val FROM: Parser[String] = """(?i)from\s""".r
    val AS: Parser[String] = """(?i)as\s""".r
    val WHERE: Parser[String] = """(?i)where\s""".r
    val GROUP: Parser[String] = """(?i)group\s""".r
    val ORDER: Parser[String] = """(?i)order\s""".r
    val BY: Parser[String] = """(?i)by\s""".r
    val DESC: Parser[String] = """(?i)desc""".r
    val ASC: Parser[String] = """(?i)asc""".r
    val HAVING: Parser[String] = """(?i)having\s""".r
    val LIMIT: Parser[String] = """(?i)limit\s""".r
  }
  import Operator._

  object Strings {
    def AnyString: Parser[String] = """"(?:[^\"]|\")*"""".r | """'(?:[^']|\')*'""".r
    def UQuoteTableFieldName: Parser[String] = """`(?:[^`]|[\\][`])*`""".r
    def TableFieldName: Parser[String] = UQuoteTableFieldName | """[a-zA-Z_]\w*""".r
    def DataSourceName: Parser[String] = genNamesParser(dataSourceNames)
    def FunctionName: Parser[String] = genNamesParser(functionNames)

    def IntegerNumber: Parser[String] = """[+\-]?\d+""".r
    def DoubleNumber: Parser[String] = """[+\-]?(\.\d+|\d+\.\d*)""".r
    def IndexNumber: Parser[String] = IntegerNumber

    def TimeString: Parser[String] = """([+\-]?\d+)(d|h|m|s|ms)""".r
    def BooleanString: Parser[String] = """(?i)true|false""".r
  }
  import Strings._

  /**
    * -- literal --
    * <literal> ::= <literal-string> | <literal-number> | <literal-time> | <literal-boolean> | <literal-null> | <literal-nan>
    * <literal-string> ::= <any-string>
    * <literal-number> ::= <integer> | <double>
    * <literal-time> ::= <integer> ("d"|"h"|"m"|"s"|"ms")
    * <literal-boolean> ::= true | false
    * <literal-null> ::= null
    * <literal-nan> ::= nan
    */
  def literal: Parser[LiteralExpr] = literalNull | literalNan | literalBoolean | literalString | literalTime | literalNumber
  def literalNull: Parser[LiteralNullExpr] = NULL ^^ { LiteralNullExpr(_) }
  def literalNan: Parser[LiteralNanExpr] = NAN ^^ { LiteralNanExpr(_) }
  def literalString: Parser[LiteralStringExpr] = AnyString ^^ { LiteralStringExpr(_) }
  def literalNumber: Parser[LiteralNumberExpr] = (DoubleNumber | IntegerNumber) ^^ { LiteralNumberExpr(_) }
  def literalTime: Parser[LiteralTimeExpr] = TimeString ^^ { LiteralTimeExpr(_) }
  def literalBoolean: Parser[LiteralBooleanExpr] = BooleanString ^^ { LiteralBooleanExpr(_) }

  /**
    * -- selection --
    * <selection> ::= <selection-head> [ <field-sel> | <index-sel> | <function-sel> ]*
    * <selection-head> ::= ("data source name registered") | <function>
    * <field-sel> ::= "." <field-name> | "[" <quote-field-name> "]"
    * <index-sel> ::= "[" <arg> "]"
    * <function-sel> ::= "." <function-name> "(" [<arg>]? [, <arg>]* ")"
    * <arg> ::= <math-expr>
    */

  def selection: Parser[SelectionExpr] = selectionHead ~ rep(selector) ~ opt(asAlias) ^^ {
    case head ~ sels ~ aliasOpt => SelectionExpr(head, sels, aliasOpt)
  }
  def selectionHead: Parser[HeadExpr] = DataSourceName ^^ {
    DataSourceHeadExpr(_)
  } | function ^^ {
    OtherHeadExpr(_)
  } | TableFieldName ^^ {
    FieldNameHeadExpr(_)
  } | ALLSL ^^ { _ =>
    ALLSelectHeadExpr()
  }
  def selector: Parser[SelectExpr] = functionSelect | allFieldsSelect | fieldSelect | indexSelect
  def allFieldsSelect: Parser[AllFieldsSelectExpr] = DOT ~> ALLSL ^^ { _ => AllFieldsSelectExpr() }
  def fieldSelect: Parser[FieldSelectExpr] = DOT ~> TableFieldName ^^ { FieldSelectExpr(_) }
  def indexSelect: Parser[IndexSelectExpr] = LSQBR ~> argument <~ RSQBR ^^ { IndexSelectExpr(_) }
  def functionSelect: Parser[FunctionSelectExpr] = DOT ~ FunctionName ~ LBR ~ repsep(argument, COMMA) ~ RBR ^^ {
    case _ ~ name ~ _ ~ args ~ _ => FunctionSelectExpr(name, args)
  }

  def asAlias: Parser[String] = AS ~> TableFieldName

  /**
    * -- math expr --
    * <math-factor> ::= <literal> | <alias-expr> | <function> | <selection> | "(" <math-expr> ")"
    * <unary-math-expr> ::= [<unary-opr>]* <math-factor>
    * <binary-math-expr> ::= <unary-math-expr> [<binary-opr> <unary-math-expr>]+
    * <math-expr> ::= <binary-math-expr>
    */

  def mathFactor: Parser[MathExpr] = (literal | function | selection) ^^ {
    MathFactorExpr(_, false, None)
  } | LBR ~ mathExpression ~ RBR ~ opt(asAlias) ^^ {
    case _ ~ expr ~ _ ~ aliasOpt => MathFactorExpr(expr, true, aliasOpt)
  }
  def unaryMathExpression: Parser[MathExpr] = rep(MATH_UNARY) ~ mathFactor ^^ {
    case Nil ~ a => a
    case list ~ a => UnaryMathExpr(list, a)
  }
  def binaryMathExpressions: Seq[Parser[MathExpr]] =
    MATH_BINARIES.foldLeft(List[Parser[MathExpr]](unaryMathExpression)) { (parsers, binaryParser) =>
      val pre = parsers.head
      val cur = pre ~ rep(binaryParser ~ pre) ^^ {
        case a ~ Nil => a
        case a ~ list => BinaryMathExpr(a, list.map(c => (c._1, c._2)))
      }
      cur :: parsers
    }
  def mathExpression: Parser[MathExpr] = binaryMathExpressions.head

  /**
    * -- logical expr --
    * <in-expr> ::= <math-expr> [<not>]? <in> <range-expr>
    * <between-expr> ::= <math-expr> [<not>]? <between> (<math-expr> <and> <math-expr> | <range-expr>)
    * <range-expr> ::= "(" [<math-expr>]? [, <math-expr>]+ ")"
    * <like-expr> ::= <math-expr> [<not>]? <like> <math-expr>
    * <is-null-expr> ::= <math-expr> <is> [<not>]? <null>
    * <is-nan-expr> ::= <math-expr> <is> [<not>]? <nan>
    *
    * <logical-factor> ::= <math-expr> | <in-expr> | <between-expr> | <like-expr> | <is-null-expr> | <is-nan-expr> | "(" <logical-expr> ")"
    * <unary-logical-expr> ::= [<unary-logical-opr>]* <logical-factor>
    * <binary-logical-expr> ::= <unary-logical-expr> [<binary-logical-opr> <unary-logical-expr>]+
    * <logical-expr> ::= <binary-logical-expr>
    */

  def inExpr: Parser[LogicalExpr] = mathExpression ~ opt(NOT) ~ IN ~ LBR ~ repsep(mathExpression, COMMA) ~ RBR ^^ {
    case head ~ notOpt ~ _ ~ _ ~ list ~ _ => InExpr(head, notOpt.isEmpty, list)
  }
  def betweenExpr: Parser[LogicalExpr] = mathExpression ~ opt(NOT) ~ BETWEEN ~ LBR ~ repsep(mathExpression, COMMA) ~ RBR ^^ {
    case head ~ notOpt ~ _ ~ _ ~ list ~ _ => BetweenExpr(head, notOpt.isEmpty, list)
  } | mathExpression ~ opt(NOT) ~ BETWEEN ~ mathExpression ~ AND_ONLY ~ mathExpression ^^ {
    case head ~ notOpt ~ _ ~ first ~ _ ~ second => BetweenExpr(head, notOpt.isEmpty, Seq(first, second))
  }
  def likeExpr: Parser[LogicalExpr] = mathExpression ~ opt(NOT) ~ LIKE ~ mathExpression ^^ {
    case head ~ notOpt ~ _ ~ value => LikeExpr(head, notOpt.isEmpty, value)
  }
  def isNullExpr: Parser[LogicalExpr] = mathExpression ~ IS ~ opt(NOT) ~ NULL ^^ {
    case head ~ _ ~ notOpt ~ _ => IsNullExpr(head, notOpt.isEmpty)
  }
  def isNanExpr: Parser[LogicalExpr] = mathExpression ~ IS ~ opt(NOT) ~ NAN ^^ {
    case head ~ _ ~ notOpt ~ _ => IsNanExpr(head, notOpt.isEmpty)
  }

  def logicalFactor: Parser[LogicalExpr] = (inExpr | betweenExpr | likeExpr | isNullExpr | isNanExpr | mathExpression) ^^ {
    LogicalFactorExpr(_, false, None)
  } | LBR ~ logicalExpression ~ RBR ~ opt(asAlias) ^^ {
    case _ ~ expr ~ _ ~ aliasOpt => LogicalFactorExpr(expr, true, aliasOpt)
  }
  def unaryLogicalExpression: Parser[LogicalExpr] = rep(LOGICAL_UNARY) ~ logicalFactor ^^ {
    case Nil ~ a => a
    case list ~ a => UnaryLogicalExpr(list, a)
  }
  def binaryLogicalExpressions: Seq[Parser[LogicalExpr]] =
    LOGICAL_BINARIES.foldLeft(List[Parser[LogicalExpr]](unaryLogicalExpression)) { (parsers, binaryParser) =>
      val pre = parsers.head
      val cur = pre ~ rep(binaryParser ~ pre) ^^ {
        case a ~ Nil => a
        case a ~ list => BinaryLogicalExpr(a, list.map(c => (c._1, c._2)))
      }
      cur :: parsers
    }
  def logicalExpression: Parser[LogicalExpr] = binaryLogicalExpressions.head

  /**
    * -- expression --
    * <expr> = <math-expr> | <logical-expr>
    */

  def expression: Parser[Expr] = logicalExpression | mathExpression

  /**
    * -- function expr --
    * <function> ::= <function-name> "(" [<arg>] [, <arg>]+ ")"
    * <function-name> ::= ("function name registered")
    * <arg> ::= <expr>
    */

  def function: Parser[FunctionExpr] = FunctionName ~ LBR ~ repsep(argument, COMMA) ~ RBR ~ opt(asAlias) ^^ {
    case name ~ _ ~ args ~ _ ~ aliasOpt => FunctionExpr(name, args, aliasOpt)
  }
  def argument: Parser[Expr] = expression

  /**
    * -- clauses --
    * <select-clause> = <expr> [, <expr>]*
    * <where-clause> = <where> <expr>
    * <having-clause> = <having> <expr>
    * <groupby-clause> = <group> <by> <expr> [ <having-clause> ]?
    * <orderby-item> = <expr> [ <DESC> ]?
    * <orderby-clause> = <order> <by> <orderby-item> [ , <orderby-item> ]*
    * <limit-clause> = <limit> <expr>
    */

  def selectClause: Parser[SelectClause] = opt(SELECT) ~> rep1sep(expression, COMMA) ^^ { SelectClause(_) }
  def fromClause: Parser[FromClause] = FROM ~> TableFieldName ^^ { ds => FromClause(trim(ds)) }
  def whereClause: Parser[WhereClause] = WHERE ~> expression ^^ { WhereClause(_) }
  def havingClause: Parser[Expr] = HAVING ~> expression
  def groupbyClause: Parser[GroupbyClause] = GROUP ~ BY ~ rep1sep(expression, COMMA) ~ opt(havingClause) ^^ {
    case _ ~ _ ~ cols ~ havingOpt => GroupbyClause(cols, havingOpt)
  }
  def orderbyItem: Parser[OrderbyItem] = expression ~ opt(DESC | ASC) ^^ {
    case expr ~ orderOpt => OrderbyItem(expr, orderOpt)
  }
  def orderbyClause: Parser[OrderbyClause] = ORDER ~ BY ~ rep1sep(orderbyItem, COMMA) ^^ {
    case _ ~ _ ~ cols => OrderbyClause(cols)
  }
  def limitClause: Parser[LimitClause] = LIMIT ~> expression ^^ { LimitClause(_) }

  /**
    * -- combined clauses --
    * <combined-clauses> = <select-clause> [ <from-clause> ]+ [ <where-clause> ]+ [ <groupby-clause> ]+ [ <orderby-clause> ]+ [ <limit-clause> ]+
    */

  def combinedClause: Parser[CombinedClause] = selectClause ~ opt(fromClause) ~ opt(whereClause) ~
    opt(groupbyClause) ~ opt(orderbyClause) ~ opt(limitClause) ^^ {
    case sel ~ fromOpt ~ whereOpt ~ groupbyOpt ~ orderbyOpt ~ limitOpt => {
      val tails = Seq(whereOpt, groupbyOpt, orderbyOpt, limitOpt).flatMap(opt => opt)
      CombinedClause(sel, fromOpt, tails)
    }
  }

}
