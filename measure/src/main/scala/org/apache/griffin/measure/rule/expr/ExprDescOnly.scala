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
package org.apache.griffin.measure.rule.expr

trait ExprDescOnly extends Describable {

}


case class SelectionHead(expr: String) extends ExprDescOnly {
  private val headRegex = """\$(\w+)""".r
  val head: String = expr match {
    case headRegex(v) => v.toLowerCase
    case _ => expr
  }
  val desc: String = "$" + head
}

case class RangeDesc(elements: Iterable[MathExpr]) extends ExprDescOnly {
  val desc: String = {
    val rangeDesc = elements.map(_.desc).mkString(", ")
    s"(${rangeDesc})"
  }
}
