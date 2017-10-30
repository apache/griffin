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

sealed trait PersistType {
  val regex: Regex
  val desc: String
//  def temp: Boolean = false
//  def persist: Boolean = false
//  def collect: Boolean = false
}

object PersistType {
  private val persistTypes: List[PersistType] = List(RecordPersistType, MetricPersistType, NonePersistType)
  def apply(ptn: String): PersistType = {
    persistTypes.filter(tp => ptn match {
      case tp.regex() => true
      case _ => false
    }).headOption.getOrElse(NonePersistType)
  }
  def unapply(pt: PersistType): Option[String] = Some(pt.desc)
}

final case object NonePersistType extends PersistType {
  val regex: Regex = "".r
  val desc: String = "none"
}

final case object RecordPersistType extends PersistType {
  val regex: Regex = "^(?i)record$".r
  val desc: String = "record"
//  override def temp: Boolean = true
}

final case object MetricPersistType extends PersistType {
  val regex: Regex = "^(?i)metric$".r
  val desc: String = "metric"
//  override def temp: Boolean = true
//  override def collect: Boolean = true
}