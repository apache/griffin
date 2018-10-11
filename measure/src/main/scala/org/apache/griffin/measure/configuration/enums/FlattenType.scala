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
sealed trait FlattenType {
  val idPattern: Regex
  val desc: String
}

object FlattenType {
  private val flattenTypes: List[FlattenType] = List(
    DefaultFlattenType,
    EntriesFlattenType,
    ArrayFlattenType,
    MapFlattenType
  )

  val default = DefaultFlattenType
  def apply(ptn: String): FlattenType = {
    flattenTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(default)
  }
  def unapply(pt: FlattenType): Option[String] = Some(pt.desc)
}

/**
  * default flatten strategy
  * metrics contains 1 row -> flatten metric json map
  * metrics contains n > 1 rows -> flatten metric json array
  * n = 0: { }
  * n = 1: { "col1": "value1", "col2": "value2", ... }
  * n > 1: { "arr-name": [ { "col1": "value1", "col2": "value2", ... }, ... ] }
  * all rows
  */
 case object DefaultFlattenType extends FlattenType {
  val idPattern: Regex = "".r
  val desc: String = "default"
}

/**
  * metrics contains n rows -> flatten metric json map
  * n = 0: { }
  * n >= 1: { "col1": "value1", "col2": "value2", ... }
  * the first row only
  */
 case object EntriesFlattenType extends FlattenType {
  val idPattern: Regex = "^(?i)entries$".r
  val desc: String = "entries"
}

/**
  * metrics contains n rows -> flatten metric json array
  * n = 0: { "arr-name": [ ] }
  * n >= 1: { "arr-name": [ { "col1": "value1", "col2": "value2", ... }, ... ] }
  * all rows
  */
 case object ArrayFlattenType extends FlattenType {
  val idPattern: Regex = "^(?i)array|list$".r
  val desc: String = "array"
}

/**
  * metrics contains n rows -> flatten metric json wrapped map
  * n = 0: { "map-name": { } }
  * n >= 1: { "map-name": { "col1": "value1", "col2": "value2", ... } }
  * the first row only
  */
 case object MapFlattenType extends FlattenType {
  val idPattern: Regex = "^(?i)map$".r
  val desc: String = "map"
}
