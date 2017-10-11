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
package org.apache.griffin.measure.rule.adaptor

import java.util.concurrent.atomic.AtomicLong


import scala.collection.mutable.{Set => MutableSet}
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.rule.step.{ConcreteRuleStep, RuleStep}
import org.apache.griffin.measure.rule.dsl.{DslType, PersistType}

trait RuleAdaptor extends Loggable with Serializable {

  val adaptPhase: AdaptPhase

  val _name = "name"
  val _rule = "rule"
  val _persistType = "persist.type"
  val _updateDataSource = "update.data.source"
  val _details = "details"

  protected def getName(param: Map[String, Any]) = param.getOrElse(_name, RuleStepNameGenerator.genName).toString
  protected def getRule(param: Map[String, Any]) = param.getOrElse(_rule, "").toString
  protected def getPersistType(param: Map[String, Any]) = PersistType(param.getOrElse(_persistType, "").toString)
  protected def getUpdateDataSource(param: Map[String, Any]) = param.get(_updateDataSource).map(_.toString)
  protected def getDetails(param: Map[String, Any]) = param.get(_details) match {
    case Some(dt: Map[String, Any]) => dt
    case _ => Map[String, Any]()
  }

  def getTempSourceNames(param: Map[String, Any]): Seq[String]

  def genRuleStep(param: Map[String, Any]): Seq[RuleStep]
  def genConcreteRuleStep(param: Map[String, Any]): Seq[ConcreteRuleStep] = {
    genRuleStep(param).flatMap { rs =>
      adaptConcreteRuleStep(rs)
    }
  }
  protected def adaptConcreteRuleStep(ruleStep: RuleStep): Seq[ConcreteRuleStep]

}

object RuleStepNameGenerator {
  private val counter: AtomicLong = new AtomicLong(0L)
  private val head: String = "rs"

  def genName: String = {
    s"${head}${increment}"
  }

  private def increment: Long = {
    counter.incrementAndGet()
  }
}