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

// result for profile: match count, total count
case class ProfileResult(matchCount: Long, totalCount: Long) extends Result {

  type T = ProfileResult

  def update(delta: T): T = {
    ProfileResult(matchCount + delta.matchCount, totalCount)
  }

  def eventual(): Boolean = {
    this.matchCount >= totalCount
  }

  def differsFrom(other: T): Boolean = {
    (this.matchCount != other.matchCount) || (this.totalCount != other.totalCount)
  }

  def getMiss = totalCount - matchCount
  def getTotal = totalCount
  def getMatch = matchCount

  def matchPercentage: Double = if (getTotal <= 0) 0 else getMatch.toDouble / getTotal * 100

}
