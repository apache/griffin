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
    * <unary-opr> ::= "-" | "+"
    *
    * <math-factor> ::= <literal> | <selection> | "(" <math-expr> ")"
    *
    * <selection> ::= <selection-head> [ <field-sel> | <function-operation> | <index-field-range-sel> | <filter-sel> ]+
    * selection example: $source.price, $source.json(), $source['state'], $source.numList[3], $target.json().mails['org' = 'apache'].names[*]
    *
    * <selection-head> ::= $source | $target
    *
    * <field-sel> ::= "." <any-string>
    *
    * <function-operation> ::= "." <function-name> "(" <arg> [, <arg>]+ ")"
    * <function-name> ::= <name-string>
    * <arg> ::= <math-expr>
    *
    * <index-field-range-sel> ::= "[" <index-field-range> [, <index-field-range>]+ "]"
    * <index-field-range> ::= <index-field> | (<index-field>, <index-field>) | "*"
    * index-field-range: 2 means the 3rd item, (0, 3) means first 4 items, * means all items, 'age' means item 'age'
    * <index-field> ::= <math-expr>
    * index: 0 ~ n means position from start, -1 ~ -n means position from end
    *
    * <filter-sel> ::= "[" <field-quote> <filter-compare-opr> <math-expr> "]"
    * <filter-compare-opr> ::= "=" | "!=" | "<" | ">" | "<=" | ">="
    * filter-sel example: ['name' = 'URL'], $source.man['age' > $source.graduate_age + 5 ]
    *
    * When <math-expr> in the selection, it mustn't contain the different <selection-head>, for example:
    * $source.tags[1+2]             valid
    * $source.tags[$source.first]   valid
    * $source.tags[$target.first]   invalid
    *
    *
    * <literal> ::= <literal-string> | <literal-number> | <literal-time> | <literal-boolean>
    * <literal-string> ::= <any-string>
    * <literal-number> ::= <integer> | <double>
    * <literal-time> ::= <integer> ("d"|"h"|"m"|"s"|"ms")
    * <literal-boolean> ::= true | false
    *
    */

  // basic
  val anyString: Parser[String] = """[^'{}\[\]()=<>.$@;+\-*/\\\"]*""".r
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
  def variableString: Parser[VariableExpr] = (("'" ~> anyString <~ "'") | ("\"" ~> anyString <~ "\"")) ^^ { VariableStringExpr(_) }
  def constString: Parser[ConstExpr] = (("'" ~> anyString <~ "'") | ("\"" ~> anyString <~ "\"")) ^^ { ConstStringExpr(_) }
  def constValue: Parser[ConstExpr] = time ^^ { ConstTimeExpr(_) } | number ^^ { ConstNumberExpr(_)} | constString
  def variableValue: Parser[VariableExpr] = variable ^^ { VariableStringExpr(_) }
  def quoteVariableValue: Parser[QuoteVariableExpr] = "${" ~> variable <~ "}" ^^ { QuoteVariableExpr(_) }
  def position: Parser[SelectExpr] = anyPosition ^^ { AnyPositionExpr(_) } | """\d+""".r ^^ { NumPositionExpr(_) } | (("'" ~> anyString <~ "'") | ("\"" ~> anyString <~ "\"")) ^^ { StringPositionExpr(_) }
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
