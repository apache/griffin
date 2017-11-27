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

case class RuleInfo(name: String, rule: String, details: Map[String, Any]) {
  private val _name = "name"
  private val _persistType = "persist.type"
  private val _asArray = "as.array"
  private val _updateDataSource = "update.data.source"

  def persistType = PersistType(details.getOrElse(_persistType, "").toString)
  def updateDataSourceOpt = details.get(_updateDataSource).map(_.toString)

  def withName(n: String): RuleInfo = {
    RuleInfo(name, rule, details + (_name -> n))
  }
  def withPersistType(pt: PersistType): RuleInfo = {
    RuleInfo(name, rule, details + (_persistType -> pt.desc))
  }
  def withUpdateDataSourceOpt(udsOpt: Option[String]): RuleInfo = {
    udsOpt match {
      case Some(uds) => RuleInfo(name, rule, details + (_updateDataSource -> uds))
      case _ => this
    }
  }

  def originName: String = {
    details.getOrElse(_name, name).toString
  }
  def asArray: Boolean = {
    try {
      details.get(_asArray) match {
        case Some(v) => v.toString.toBoolean
        case _ => false
      }
    } catch {
      case e: Throwable => false
    }
  }
}