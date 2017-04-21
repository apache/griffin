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
  * <field-quote> ::= ' <field-string> ' | " <field-string> "
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


/**
  * BACK_UP
  *
  * <index-field-range-sel> ::= "[" <index-field-range> [, <index-field-range>]+ "]"
  * <index-field-range> ::= <index-field> | (<index-field>, <index-field>) | "*"
  * index-field-range: 2 means the 3rd item, (0, 3) means first 4 items, * means all items, 'age' means item 'age'
  * <index-field> ::= <index> | <field-quote> | <math-result>
  * <field-quote> ::= ' <any-string> ' | \" <any-string> \"
  * <index> ::= <integer>
  * index: 0 ~ n means position from start, -1 ~ -n means position from end
  * <math-result> ::= "${" <math-expr> "}"
  *
  * <filter-sel> ::= "[" <field-quote> <filter-compare-opr> <filter-value> "]"
  * <filter-compare-opr> ::= "=" | "!=" | "<" | ">" | "<=" | ">="
  * <filter-value> ::= <literal> | <math-result>
  * filter-sel example: ['name' = 'URL'], $source.man['age' > ${ $source.graduate_age + 5 }]
  *
  */
