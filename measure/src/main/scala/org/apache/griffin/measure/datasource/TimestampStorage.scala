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
package org.apache.griffin.measure.datasource

import org.apache.griffin.measure.Loggable

import scala.collection.mutable.{SortedSet => MutableSortedSet}

/**
  * tmst cache, CRUD of timestamps
  */
case class TimestampStorage() extends Loggable {

  private val tmstGroup: MutableSortedSet[Long] = MutableSortedSet.empty[Long]

  //-- insert tmst into tmst group --
  def insert(tmst: Long) = tmstGroup += tmst
  def insert(tmsts: Iterable[Long]) = tmstGroup ++= tmsts

  //-- remove tmst from tmst group --
  def remove(tmst: Long) = tmstGroup -= tmst
  def remove(tmsts: Iterable[Long]) = tmstGroup --= tmsts

  //-- get subset of tmst group --
  def fromUntil(from: Long, until: Long) = tmstGroup.range(from, until).toSet
  def afterTil(after: Long, til: Long) = tmstGroup.range(after + 1, til + 1).toSet
  def until(until: Long) = tmstGroup.until(until).toSet
  def from(from: Long) = tmstGroup.from(from).toSet
  def all = tmstGroup.toSet

}