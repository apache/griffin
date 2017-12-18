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
package org.apache.griffin.measure.rule.step

trait TimeInfo extends Serializable {
  val calcTime: Long
  val tmst: Long
  val head: String

  def key: String = if (head.nonEmpty) s"${head}_${calcTime}" else s"${calcTime}"
  def setHead(h: String): TimeInfo
}

case class CalcTimeInfo(calcTime: Long, head: String = "") extends TimeInfo {
  val tmst: Long = calcTime
  def setHead(h: String): TimeInfo = CalcTimeInfo(calcTime, h)
}

case class TmstTimeInfo(calcTime: Long, tmst: Long, head: String = "") extends TimeInfo {
  def setHead(h: String): TimeInfo = TmstTimeInfo(calcTime, tmst, h)
}