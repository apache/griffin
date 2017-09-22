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
package org.apache.griffin.measure.rule.dsl.analyzer

import org.apache.griffin.measure.rule.dsl.expr._


trait BasicAnalyzer extends Serializable {

  val expr: Expr

  val seqDataSourceNames = (expr: Expr, v: Set[String]) => {
    expr match {
      case DataSourceHeadExpr(name) => v + name
      case _ => v
    }
  }
  val combDataSourceNames = (a: Set[String], b: Set[String]) => a ++ b

  val seqSelectionExprs = (dsName: String) => (expr: Expr, v: Seq[SelectionExpr]) => {
    expr match {
      case se @ SelectionExpr(head: DataSourceHeadExpr, _, _) if (head.desc == dsName) => v :+ se
      case _ => v
    }
  }
  val combSelectionExprs = (a: Seq[SelectionExpr], b: Seq[SelectionExpr]) => a ++ b

  val seqWithAliasExprs = (expr: Expr, v: Seq[AliasableExpr]) => {
    expr match {
      case se: SelectExpr => v
      case a: AliasableExpr if (a.alias.nonEmpty) => v :+ a
      case _ => v
    }
  }
  val combWithAliasExprs = (a: Seq[AliasableExpr], b: Seq[AliasableExpr]) => a ++ b

}
