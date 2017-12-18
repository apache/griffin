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

case class RuleInfo(name: String, tmstNameOpt: Option[String], dslType: DslType,
                    rule: String, details: Map[String, Any], gather: Boolean) {

  val persistName = details.getString(_persistName, name)
  val persistType = PersistType(details.getString(_persistType, ""))
  val collectType = CollectType(details.getString(_collectType, ""))
  val cacheDataSourceOpt = details.get(_cacheDataSource).map(_.toString)

  def setName(n: String): RuleInfo = {
    RuleInfo(n, tmstNameOpt, dslType, rule, details, gather)
  }
  def setTmstNameOpt(tnOpt: Option[String]): RuleInfo = {
    RuleInfo(name, tnOpt, dslType, rule, details, gather)
  }
  def setDslType(dt: DslType): RuleInfo = {
    RuleInfo(name, tmstNameOpt, dt, rule, details, gather)
  }
  def setRule(r: String): RuleInfo = {
    RuleInfo(name, tmstNameOpt, dslType, r, details, gather)
  }
  def setDetails(d: Map[String, Any]): RuleInfo = {
    RuleInfo(name, tmstNameOpt, dslType, rule, d, gather)
  }
  def setGather(g: Boolean): RuleInfo = {
    RuleInfo(name, tmstNameOpt, dslType, rule, details, g)
  }

  def getNames: Seq[String] = {
    tmstNameOpt match {
      case Some(tn) => name :: tn :: Nil
      case _ => name :: Nil
    }
  }
}

