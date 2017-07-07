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

import scala.util.{Success, Try}

trait FieldDescOnly extends Describable with DataSourceable {

}

case class IndexDesc(expr: String) extends FieldDescOnly {
  val index: Int = {
    Try(expr.toInt) match {
      case Success(v) => v
      case _ => throw new Exception(s"${expr} is invalid index")
    }
  }
  val desc: String = describe(index)
  val dataSources: Set[String] = Set.empty[String]
}

case class FieldDesc(expr: String) extends FieldDescOnly {
  val field: String = expr
  val desc: String = describe(field)
  val dataSources: Set[String] = Set.empty[String]
}

case class AllFieldsDesc(expr: String) extends FieldDescOnly {
  val allFields: String = expr
  val desc: String = allFields
  val dataSources: Set[String] = Set.empty[String]
}

case class FieldRangeDesc(startField: FieldDescOnly, endField: FieldDescOnly) extends FieldDescOnly {
  val desc: String = {
    (startField, endField) match {
      case (f1: IndexDesc, f2: IndexDesc) => s"(${f1.desc}, ${f2.desc})"
      case _ => throw new Exception("invalid field range description")
    }
  }
  val dataSources: Set[String] = Set.empty[String]
}