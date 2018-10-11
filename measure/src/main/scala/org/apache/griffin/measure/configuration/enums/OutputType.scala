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
  * the strategy to flatten metric
  */
sealed trait OutputType {
  val idPattern: Regex
  val desc: String
}

object OutputType {
  private val outputTypes: List[OutputType] = List(
    MetricOutputType,
    RecordOutputType,
    DscUpdateOutputType,
    UnknownOutputType
  )

  val default = UnknownOutputType
  def apply(ptn: String): OutputType = {
    outputTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(default)
  }
  def unapply(pt: OutputType): Option[String] = Some(pt.desc)
}

/**
  * metric output type
  * output the rule step result as metric
  */
 case object MetricOutputType extends OutputType {
  val idPattern: Regex = "^(?i)metric$".r
  val desc: String = "metric"
}

/**
  * record output type
  * output the rule step result as records
  */
 case object RecordOutputType extends OutputType {
  val idPattern: Regex = "^(?i)record|records$".r
  val desc: String = "record"
}

/**
  * data source cache update output type
  * output the rule step result to update data source cache
  */
 case object DscUpdateOutputType extends OutputType {
  val idPattern: Regex = "^(?i)dsc-update$".r
  val desc: String = "dsc-update"
}

/**
  * unknown output type
  * will not output the result
  */
 case object UnknownOutputType extends OutputType {
  val idPattern: Regex = "".r
  val desc: String = "unknown"
}
