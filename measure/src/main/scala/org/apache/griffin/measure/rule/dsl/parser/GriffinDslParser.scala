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

import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.dsl.expr._

case class GriffinDslParser(dataSourceNames: Seq[String], functionNames: Seq[String]
                           ) extends BasicParser {

  /**
    * -- profiling clauses --
    * <profiling-clauses> = <select-clause> [ <from-clause> ]+ [ <where-clause> ]+ [ <groupby-clause> ]+ [ <orderby-clause> ]+ [ <limit-clause> ]+
    */

  def profilingClause: Parser[ProfilingClause] = selectClause ~ opt(fromClause) ~ opt(whereClause) ~
    opt(groupbyClause) ~ opt(orderbyClause) ~ opt(limitClause) ^^ {
    case sel ~ fromOpt ~ whereOpt ~ groupbyOpt ~ orderbyOpt ~ limitOpt => {
      val preClauses = Seq(whereOpt).flatMap(opt => opt)
      val postClauses = Seq(orderbyOpt, limitOpt).flatMap(opt => opt)
      ProfilingClause(sel, fromOpt, groupbyOpt, preClauses, postClauses)
    }
  }

  def parseRule(rule: String, dqType: DqType): ParseResult[Expr] = {
    val rootExpr = dqType match {
      case AccuracyType => logicalExpression
      case ProfilingType => profilingClause
      case _ => expression
    }
    parseAll(rootExpr, rule)
  }

}
