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
    * <literal> ::= <literal-string> | <literal-number> | <literal-time> | <literal-boolean>
    * <literal-string> ::= <any-string>
    * <literal-number> ::= <integer> | <double>
    * <literal-time> ::= <integer> ("d"|"h"|"m"|"s"|"ms")
    * <literal-boolean> ::= true | false
    *
    */

  object Keyword {
    def UnaryLogicalKeywords: Parser[String] = """(?i)not""".r
    def BinaryLogicalKeywords: Parser[String] = """(?i)and|or""".r
    def RangeKeywords: Parser[String] = """(?i)(not\s+)?(in|between)""".r
    def DataSourceKeywords: Parser[String] = """(?i)\$(source|target)""".r
    def Keywords: Parser[String] = UnaryLogicalKeywords | BinaryLogicalKeywords | RangeKeywords | DataSourceKeywords
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
    def AnyString: Parser[String] = """.*""".r
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
  def literal: Parser[String] = literialString | literialNumber | literialTime | literialBoolean
  def literialString: Parser[String] = (SQuote ~> AnyString <~ SQuote) | (DQuote ~> AnyString <~ DQuote)
  def literialNumber: Parser[String] = IntegerNumber | DoubleNumber
  def literialTime: Parser[String] = """(\d+(d|h|m|s|ms))+""".r
  def literialBoolean: Parser[String] = """(?i)true""".r | """(?i)false""".r

  // -- selection --
  // <selection> ::= <selection-head> [ <field-sel> | <function-operation> | <index-field-range-sel> | <filter-sel> ]+
  def selection: Parser[String] = selectionHead ~ rep(selector) ^^ {
    case head ~ tails => head + "[" + tails.mkString("") + "]"
  }
  def selector: Parser[String] = (fieldSelect | functionOperation | indexFieldRangeSelect | filterSelect)

  def selectionHead: Parser[String] = DataSourceKeywords
  // <field-sel> ::= "." <field-string>
  def fieldSelect: Parser[String] = Dot ~> SimpleFieldString
  // <function-operation> ::= "." <function-name> "(" <arg> [, <arg>]+ ")"
  def functionOperation: Parser[String] = Dot ~ NameString ~ BracketPair._1 ~ repsep(argument, Comma) ~ BracketPair._2 ^^ {
    case _ ~ func ~ _ ~ args ~ _ => s".${func}(${args.mkString("")})"
  }
  def argument: Parser[String] = mathExpr
  // <index-field-range-sel> ::= "[" <index-field-range> [, <index-field-range>]+ "]"
  def indexFieldRangeSelect: Parser[String] = SqBracketPair._1 ~> rep1sep(indexFieldRange, Comma) <~ SqBracketPair._2 ^^ {
    case ifrs => s"${ifrs.mkString("")}"
  }
  // <index-field-range> ::= <index-field> | (<index-field>, <index-field>) | "*"
  def indexFieldRange: Parser[String] = indexField | BracketPair._1 ~ indexField ~ Comma ~ indexField ~ BracketPair._2 ^^ {
    case _ ~ if1 ~ _ ~ if2 ~ _ => s"(${if1},${if2})"
  }
  // <index-field> ::= <index> | <field-quote> | <all-selection>
  def indexField: Parser[String] = IndexNumber | fieldQuote | AllSelection
  // <field-quote> ::= ' <field-string> ' | " <field-string> "
  def fieldQuote: Parser[String] = (SQuote ~> FieldString <~ SQuote) | (DQuote ~> FieldString <~ DQuote)
  // <filter-sel> ::= "[" <field-quote> <filter-compare-opr> <math-expr> "]"
  def filterSelect: Parser[String] = SqBracketPair._1 ~> fieldQuote ~ FilterCompareOpr ~ mathExpr <~ SqBracketPair._2 ^^ {
    case field ~ compare ~ value => s"${field} ${compare} ${value}"
  }

  // -- math --
  // <math-factor> ::= <literal> | <selection> | "(" <math-expr> ")"
  def mathFactor: Parser[String] = literal | selection | BracketPair._1 ~> mathExpr <~ BracketPair._2 ^^ {
    case a => s"(${a})"
  }
  // <math-expr> ::= [<unary-opr>] <math-factor> [<binary-opr> <math-factor>]+
  // <unary-opr> ::= "+" | "-"
  def unaryMathExpr: Parser[String] = opt(UnaryMathOpr) ~ mathFactor ^^ {
    case Some(unary) ~ factor => s"${unary}${factor}"
    case _ ~ factor => factor
  }
  // <binary-opr> ::= "+" | "-" | "*" | "/" | "%"
  def binaryMathExpr1: Parser[String] = unaryMathExpr ~ rep(BinaryMathOpr1 ~ unaryMathExpr) ^^ {
    case a ~ Nil => a
    case a ~ list => s"${a} ${list.map(c => s"${c._1} ${c._2}").mkString(" ")}"
  }
  def binaryMathExpr2: Parser[String] = binaryMathExpr1 ~ rep(BinaryMathOpr2 ~ binaryMathExpr1) ^^ {
    case a ~ Nil => a
    case a ~ list => s"${a} ${list.map(c => s"${c._1} ${c._2}").mkString(" ")}"
  }
  def mathExpr: Parser[String] = binaryMathExpr2

  // -- logical expression --
  // <range-expr> ::= "(" [<math-expr>] [, <math-expr>]+ ")"
  def rangeExpr: Parser[String] = BracketPair._1 ~> rep1sep(mathExpr, Comma) <~ BracketPair._2 ^^ {
    case list => s"(${list.mkString(", ")})"
  }
  // <logical-expression> ::= <math-expr> (<compare-opr> <math-expr> | <range-opr> <range-expr>)
  def logicalExpr: Parser[String] = mathExpr ~ CompareOpr ~ mathExpr ^^ {
    case expr1 ~ opr ~ expr2 => s"${expr1} ${opr} ${expr2}"
  } | mathExpr ~ RangeOpr ~ rangeExpr ^^ {
    case expr ~ opr ~ range => s"${expr} ${opr} ${range}"
  }

  // -- logical statement --
  def logicalFactor: Parser[String] = logicalExpr | BracketPair._1 ~> logicalStatement <~ BracketPair._2 ^^ {
    case a => s"(${a})"
  }
  def NotLogicalStatement: Parser[String] = opt(NotLogicalOpr) ~ logicalFactor ^^ {
    case Some(unary) ~ expr => s"${unary} ${expr}"
    case _ ~ expr => expr
  }
  def andLogicalStatement: Parser[String] = NotLogicalStatement ~ rep(AndLogicalOpr ~ NotLogicalStatement) ^^ {
    case 
  }
  // <logical-statement> ::= [NOT] <logical-expression> [(AND | OR) <logical-expression>]+ | "(" <logical-statement> ")"
  def logicalStatement: Parser[String] = UnaryLogicalOpr


  // for complie only
  case class NullStatementExpr(expression: String) extends StatementExpr {
    def genValue(values: Map[String, Any]): Option[Any] = None
    def getDataRelatedExprs(dataSign: String): Iterable[DataExpr] = Nil
  }
  def statementsExpr = mathExpr ^^ { NullStatementExpr(_) }


//
//  // basic
//  val anyString: Parser[String] = """[^'{}\[\]()=<>.$@;+\-*/\\\"]*""".r
//  val variable: Parser[String] = """[a-zA-Z_]\w*""".r
//  val number: Parser[String] = """[+\-]?\d+""".r
//  val time: Parser[String] = """\d+(y|M|w|d|h|m|s|ms)""".r
//
//  val numPosition: Parser[String] = """\d+""".r
//  val anyPosition: Parser[String] = "*"
//
//  val filterOpr: Parser[String] = "=" | "!=" | ">" | "<" | ">=" | "<="
//
//  val opr1: Parser[String] = "*" | "/" | "%"
//  val opr2: Parser[String] = "+" | "-"
//
//  val assignOpr: Parser[String] = "="
//  val compareOpr: Parser[String] = "==" | "!=" | ">" | "<" | ">=" | "<="
//  val mappingOpr: Parser[String] = "==="
//
//  val exprSep: Parser[String] = ";"
//
//  // simple
//  def variableString: Parser[VariableExpr] = (("'" ~> anyString <~ "'") | ("\"" ~> anyString <~ "\"")) ^^ { VariableStringExpr(_) }
//  def constString: Parser[ConstExpr] = (("'" ~> anyString <~ "'") | ("\"" ~> anyString <~ "\"")) ^^ { ConstStringExpr(_) }
//  def constValue: Parser[ConstExpr] = time ^^ { ConstTimeExpr(_) } | number ^^ { ConstNumberExpr(_)} | constString
//  def variableValue: Parser[VariableExpr] = variable ^^ { VariableStringExpr(_) }
//  def quoteVariableValue: Parser[QuoteVariableExpr] = "${" ~> variable <~ "}" ^^ { QuoteVariableExpr(_) }
//  def position: Parser[SelectExpr] = anyPosition ^^ { AnyPositionExpr(_) } | """\d+""".r ^^ { NumPositionExpr(_) } | (("'" ~> anyString <~ "'") | ("\"" ~> anyString <~ "\"")) ^^ { StringPositionExpr(_) }
//  def argument: Parser[ConstExpr] = constValue
//  def annotationExpr: Parser[AnnotationExpr] = "@" ~> variable ^^ { AnnotationExpr(_) }
//
//  // selector
//  def filterOpration: Parser[SelectExpr] = (variableString ~ filterOpr ~ constString) ^^ {
//    case v ~ opr ~ c => FilterOprExpr(opr, v, c)
//  }
//  def positionExpr: Parser[SelectExpr] = "[" ~> (filterOpration | position) <~ "]"
//  def functionExpr: Parser[SelectExpr] = "." ~ variable ~ "(" ~ repsep(argument, ",") ~ ")" ^^ {
//    case _ ~ v ~ _ ~ args ~ _ => FunctionExpr(v, args)
//  }
//  def selectorExpr: Parser[SelectExpr] = positionExpr | functionExpr
//
//  // data
//  def selectorsExpr: Parser[DataExpr] = quoteVariableValue ~ rep(selectorExpr) ^^ {
//    case q ~ tails => SelectionExpr(q, tails)
//  }
//
//  // calculation
//  def factor: Parser[ElementExpr] = (constValue | selectorsExpr | "(" ~> expr <~ ")") ^^ { FactorExpr(_) }
//  def term: Parser[ElementExpr] = factor ~ rep(opr1 ~ factor) ^^ {
//    case a ~ Nil => a
//    case a ~ list => CalculationExpr(a, list.map(c => (c._1, c._2)))
//  }
//  def expr: Parser[ElementExpr] = term ~ rep(opr2 ~ term) ^^ {
//    case a ~ Nil => a
//    case a ~ list => CalculationExpr(a, list.map(c => (c._1, c._2)))
//  }
//
//  // statement
//  def assignExpr: Parser[StatementExpr] = variableValue ~ assignOpr ~ expr ^^ {
//    case v ~ opr ~ c => AssignExpr(opr, v, c)
//  }
//  def conditionExpr: Parser[StatementExpr] = rep(annotationExpr) ~ expr ~ compareOpr ~ expr ^^ {
//    case anos ~ le ~ opr ~ re => ConditionExpr(opr, le, re, anos)
//  }
//  def mappingExpr: Parser[StatementExpr] = rep(annotationExpr) ~ expr ~ mappingOpr ~ expr ^^ {
//    case anos ~ le ~ opr ~ re => MappingExpr(opr, le, re, anos)
//  }
//  def statementExpr: Parser[StatementExpr] = assignExpr | conditionExpr | mappingExpr
//
//  // statements
//  def statementsExpr: Parser[StatementExpr] = repsep(statementExpr, exprSep) ^^ { StatementsExpr(_) }

}
