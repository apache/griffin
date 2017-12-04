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

import java.util.concurrent.atomic.AtomicLong

import org.apache.griffin.measure.rule.dsl._

trait RuleStep extends Serializable {

  val dslType: DslType

  val timeInfo: TimeInfo

  val ruleInfo: RuleInfo

  def name = ruleInfo.name

//  val name: String
//  val rule: String
//  val details: Map[String, Any]

}

case class TimeInfo(calcTime: Long, tmst: Long) {}

object RuleDetailKeys {
  val _persistName = "persist.name"
  val _persistType = "persist.type"
  val _collectType = "collect.type"
  val _cacheDataSource = "cache.data.source"
}
import RuleDetailKeys._
import org.apache.griffin.measure.utils.ParamUtil._

case class RuleInfo(name: String, rule: String, details: Map[String, Any]) {

  def persistName = details.getString(_persistName, name)
  def persistType = PersistType(details.getString(_persistType, ""))
  def collectType = CollectType(details.getString(_collectType, ""))
  def cacheDataSourceOpt = details.get(_cacheDataSource).map(_.toString)

  def withPersistName(n: String): RuleInfo = {
    RuleInfo(name, rule, details + (_persistName -> n))
  }
  def withPersistType(pt: PersistType): RuleInfo = {
    RuleInfo(name, rule, details + (_persistType -> pt.desc))
  }
  def withCollectType(ct: CollectType): RuleInfo = {
    RuleInfo(name, rule, details + (_collectType -> ct.desc))
  }
  def withCacheDataSourceOpt(udsOpt: Option[String]): RuleInfo = {
    udsOpt.map(uds => RuleInfo(name, rule, details + (_cacheDataSource -> uds))).getOrElse(this)
  }
}

