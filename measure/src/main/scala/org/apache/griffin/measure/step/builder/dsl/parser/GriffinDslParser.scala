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
package org.apache.griffin.measure.step.builder.dsl.parser

import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.step.builder.dsl._
import org.apache.griffin.measure.step.builder.dsl.expr._

/**
  * parser for griffin dsl rule
  */
case class GriffinDslParser(dataSourceNames: Seq[String], functionNames: Seq[String]
                           ) extends BasicParser {

  import Operator._

  /**
    * -- profiling clauses --
    * <profiling-clauses> = <select-clause> [ <from-clause> ]+ [ <where-clause> ]+ [ <groupby-clause> ]+ [ <orderby-clause> ]+ [ <limit-clause> ]+
    */

  def profilingClause: Parser[ProfilingClause] = selectClause ~ opt(fromClause) ~ opt(whereClause) ~
    opt(groupbyClause) ~ opt(orderbyClause) ~ opt(limitClause) ^^ {
    case sel ~ fromOpt ~ whereOpt ~ groupbyOpt ~ orderbyOpt ~ limitOpt =>
      val preClauses = Seq(whereOpt).flatMap(opt => opt)
      val postClauses = Seq(orderbyOpt, limitOpt).flatMap(opt => opt)
      ProfilingClause(sel, fromOpt, groupbyOpt, preClauses, postClauses)
  }

  /**
    * -- uniqueness clauses --
    * <uniqueness-clauses> = <expr> [, <expr>]+
    */
  def uniquenessClause: Parser[UniquenessClause] = rep1sep(expression, Operator.COMMA) ^^ {
    case exprs => UniquenessClause(exprs)
  }

  /**
    * -- distinctness clauses --
    * <sqbr-expr> = "[" <expr> "]"
    * <dist-expr> = <sqbr-expr> | <expr>
    * <distinctness-clauses> = <distExpr> [, <distExpr>]+
    */
  def sqbrExpr: Parser[Expr] = LSQBR ~> expression <~ RSQBR ^^ {
    case expr => expr.tag = "[]"; expr
  }
  def distExpr: Parser[Expr] = expression | sqbrExpr
  def distinctnessClause: Parser[DistinctnessClause] = rep1sep(distExpr, Operator.COMMA) ^^ {
    case exprs => DistinctnessClause(exprs)
  }

  /**
    * -- timeliness clauses --
    * <timeliness-clauses> = <expr> [, <expr>]+
    */
  def timelinessClause: Parser[TimelinessClause] = rep1sep(expression, Operator.COMMA) ^^ {
    case exprs => TimelinessClause(exprs)
  }

  /**
    * -- completeness clauses --
    * <completeness-clauses> = <expr> [, <expr>]+
    */
  def completenessClause: Parser[CompletenessClause] = rep1sep(expression, Operator.COMMA) ^^ {
    case exprs => CompletenessClause(exprs)
  }

  def parseRule(rule: String, dqType: DqType): ParseResult[Expr] = {
    val rootExpr = dqType match {
      case AccuracyType => logicalExpression
      case ProfilingType => profilingClause
      case UniquenessType => uniquenessClause
      case DistinctnessType => distinctnessClause
      case TimelinessType => timelinessClause
      case CompletenessType => completenessClause
      case _ => expression
    }
    parseAll(rootExpr, rule)
  }

}
