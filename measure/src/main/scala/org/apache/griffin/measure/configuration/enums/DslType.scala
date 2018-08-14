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
package org.apache.griffin.measure.configuration.enums

import scala.util.matching.Regex

/**
  * dsl type indicates the language type of rule param
  */
sealed trait DslType {
  val idPattern: Regex
  val desc: String
}

object DslType {
  private val dslTypes: List[DslType] = List(SparkSqlType, GriffinDslType, DataFrameOpsType)
  def apply(ptn: String): DslType = {
    dslTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(GriffinDslType)
  }
  def unapply(pt: DslType): Option[String] = Some(pt.desc)
}

/**
  * spark-sql: rule defined in "SPARK-SQL" directly
  */
 case object SparkSqlType extends DslType {
  val idPattern = "^(?i)spark-?sql$".r
  val desc = "spark-sql"
}

/**
  * df-ops: data frame operations rule, support some pre-defined data frame ops
  */
 case object DataFrameOpsType extends DslType {
  val idPattern = "^(?i)df-?(?:ops|opr|operations)$".r
  val desc = "df-ops"
}

/**
  * griffin-dsl: griffin dsl rule, to define dq measurements easier
  */
 case object GriffinDslType extends DslType {
  val idPattern = "^(?i)griffin-?dsl$".r
  val desc = "griffin-dsl"
}
