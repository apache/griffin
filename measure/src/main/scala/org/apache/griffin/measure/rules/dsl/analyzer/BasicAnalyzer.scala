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
package org.apache.griffin.measure.rules.dsl.analyzer

import org.apache.griffin.measure.rules.dsl.expr._


trait BasicAnalyzer extends Serializable {

  val expr: Expr

  val seqDataSourceNames = (expr: Expr, v: Set[String]) => {
    expr match {
      case DataSourceHeadExpr(name) => v + name
      case _ => v
    }
  }
  val combDataSourceNames = (a: Set[String], b: Set[String]) => a ++ b

  val seqSelectionExprs = (dsName: String) => (expr: Expr, v: Seq[Expr]) => {
    expr match {
      case se @ SelectionExpr(head: DataSourceHeadExpr, _) => {
        head.alias match {
          case Some(a) if (a == dsName) => v :+ se
          case _ => v
        }
      }
      case _ => v
    }
  }
  val combSelectionExprs = (a: Seq[Expr], b: Seq[Expr]) => a ++ b

}
