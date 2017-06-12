/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.result


sealed trait ResultInfo {
  type T
  val key: String
  val tp: String
  def wrap(value: T) = (key -> value)
}

final case object TimeGroupInfo extends ResultInfo {
  type T = Long
  val key = "__time__"
  val tp = "bigint"
}

final case object NextFireTimeInfo extends ResultInfo {
  type T = Long
  val key = "__next_fire_time__"
  val tp = "bigint"
}

final case object MismatchInfo extends ResultInfo {
  type T = String
  val key = "__mismatch__"
  val tp = "string"
}

final case object TargetInfo extends ResultInfo {
  type T = Map[String, Any]
  val key = "__target__"
  val tp = "map"
}

final case object ErrorInfo extends ResultInfo {
  type T = String
  val key = "__error__"
  val tp = "string"
}
