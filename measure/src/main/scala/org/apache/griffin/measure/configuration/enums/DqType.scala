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
  * effective when dsl type is "griffin-dsl",
  * indicates the dq type of griffin pre-defined measurements
  */
sealed trait DqType {
  val idPattern: Regex
  val desc: String
}

object DqType {
  private val dqTypes: List[DqType] = List(
    AccuracyType,
    ProfilingType,
    UniquenessType,
    DistinctnessType,
    TimelinessType,
    CompletenessType,
    UnknownType
  )
  def apply(ptn: String): DqType = {
    dqTypes.find(dqType => ptn match {
      case dqType.idPattern() => true
      case _ => false
    }).getOrElse(UnknownType)
  }
  def unapply(pt: DqType): Option[String] = Some(pt.desc)
}

/**
  * accuracy: the match percentage of items between source and target
  * count(source items matched with the ones from target) / count(source)
  * e.g.: source [1, 2, 3, 4, 5], target: [1, 2, 3, 4]
  *       metric will be: { "total": 5, "miss": 1, "matched": 4 }
  *       accuracy is 80%.
  */
 case object AccuracyType extends DqType {
  val idPattern = "^(?i)accuracy$".r
  val desc = "accuracy"
}

/**
  * profiling: the statistic data of data source
  * e.g.: max, min, average, group by count, ...
  */
 case object ProfilingType extends DqType {
  val idPattern = "^(?i)profiling$".r
  val desc = "profiling"
}

/**
  * uniqueness: the uniqueness of data source comparing with itself
  * count(unique items in source) / count(source)
  * e.g.: [1, 2, 3, 3] -> { "unique": 2, "total": 4, "dup-arr": [ "dup": 1, "num": 1 ] }
  * uniqueness indicates the items without any replica of data
  */
 case object UniquenessType extends DqType {
  val idPattern = "^(?i)uniqueness|duplicate$".r
  val desc = "uniqueness"
}

/**
  * distinctness: the distinctness of data source comparing with itself
  * count(distinct items in source) / count(source)
  * e.g.: [1, 2, 3, 3] -> { "dist": 3, "total": 4, "dup-arr": [ "dup": 1, "num": 1 ] }
  * distinctness indicates the valid information of data
  * comparing with uniqueness, distinctness is more meaningful
  */
 case object DistinctnessType extends DqType {
  val idPattern = "^(?i)distinct$".r
  val desc = "distinct"
}

/**
  * timeliness: the latency of data source with timestamp information
  * e.g.: (receive_time - send_time)
  * timeliness can get the statistic metric of latency, like average, max, min, percentile-value,
  * even more, it can record the items with latency above threshold you configured
  */
 case object TimelinessType extends DqType {
  val idPattern = "^(?i)timeliness$".r
  val desc = "timeliness"
}

/**
  * completeness: the completeness of data source
  * the columns you measure is incomplete if it is null
  */
 case object CompletenessType extends DqType {
  val idPattern = "^(?i)completeness$".r
  val desc = "completeness"
}

 case object UnknownType extends DqType {
  val idPattern = "".r
  val desc = "unknown"
}
