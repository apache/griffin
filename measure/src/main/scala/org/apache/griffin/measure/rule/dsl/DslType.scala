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
package org.apache.griffin.measure.rule.dsl

import scala.util.matching.Regex


sealed trait DslType {
  val regex: Regex
  val desc: String
}

object DslType {
  private val dslTypes: List[DslType] = List(SparkSqlType, GriffinDslType, DfOprType, UnknownDslType)
  def apply(ptn: String): DslType = {
    dslTypes.filter(tp => ptn match {
      case tp.regex() => true
      case _ => false
    }).headOption.getOrElse(UnknownDslType)
  }
  def unapply(pt: DslType): Option[String] = Some(pt.desc)
}

final case object SparkSqlType extends DslType {
  val regex = "^(?i)spark-?sql$".r
  val desc = "spark-sql"
}

final case object DfOprType extends DslType {
  val regex = "^(?i)df-?opr$".r
  val desc = "df-opr"
}

final case object GriffinDslType extends DslType {
  val regex = "^(?i)griffin-?dsl$".r
  val desc = "griffin-dsl"
}

final case object UnknownDslType extends DslType {
  val regex = "".r
  val desc = "unknown"
}