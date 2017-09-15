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
package org.apache.griffin.measure.result


sealed trait DataInfo {
  type T
  val key: String
  def wrap(value: T) = (key -> value)
  def defWrap() = wrap(dfv)
  val dfv: T
}

final case object TimeStampInfo extends DataInfo {
  type T = Long
  val key = "_tmst_"
  val dfv = 0L
}

final case object MismatchInfo extends DataInfo {
  type T = String
  val key = "_mismatch_"
  val dfv = ""
}

final case object ErrorInfo extends DataInfo {
  type T = String
  val key = "_error_"
  val dfv = ""
}

object DataInfo {
  val cacheInfoList = List(TimeStampInfo, MismatchInfo, ErrorInfo)
}