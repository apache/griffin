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

// result for accuracy: miss count, total count
case class AccuracyResult(miss: Long, total: Long) extends Result {

  type T = AccuracyResult

  def update(delta: T): T = {
    AccuracyResult(delta.miss, total)
  }

  def eventual(): Boolean = {
    this.miss <= 0
  }

  def differsFrom(other: T): Boolean = {
    (this.miss != other.miss) || (this.total != other.total)
  }

  def getMiss = miss
  def getTotal = total
  def getMatch = total - miss

  def matchPercentage: Double = if (getTotal <= 0) 0 else getMatch.toDouble / getTotal * 100

}
