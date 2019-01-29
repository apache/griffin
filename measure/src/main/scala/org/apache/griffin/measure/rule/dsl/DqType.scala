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


sealed trait DqType {
  val regex: Regex
  val desc: String
}

object DqType {
  private val dqTypes: List[DqType] = List(
    AccuracyType, ProfilingType, UniquenessType, DistinctnessType, TimelinessType, CompletenessType, UnknownType
  )
  def apply(ptn: String): DqType = {
    dqTypes.find(tp => ptn match {
      case tp.regex() => true
      case _ => false
    }).getOrElse(UnknownType)
  }
  def unapply(pt: DqType): Option[String] = Some(pt.desc)
}

final case object AccuracyType extends DqType {
  val regex = "^(?i)accuracy$".r
  val desc = "accuracy"
}

final case object ProfilingType extends DqType {
  val regex = "^(?i)profiling$".r
  val desc = "profiling"
}

final case object UniquenessType extends DqType {
  val regex = "^(?i)uniqueness|duplicate$".r
  val desc = "uniqueness"
}

final case object DistinctnessType extends DqType {
  val regex = "^(?i)distinct$".r
  val desc = "distinct"
}

final case object TimelinessType extends DqType {
  val regex = "^(?i)timeliness$".r
  val desc = "timeliness"
}

final case object CompletenessType extends DqType {
  val regex = "^(?i)completeness$".r
  val desc = "completeness"
}

final case object UnknownType extends DqType {
  val regex = "".r
  val desc = "unknown"
}