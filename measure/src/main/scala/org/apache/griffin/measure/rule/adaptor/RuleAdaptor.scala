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
import org.apache.griffin.measure.rule.step._
import org.apache.griffin.measure.rule.dsl._

trait RuleAdaptor extends Loggable with Serializable {

  val adaptPhase: AdaptPhase

  protected def genRuleInfo(param: Map[String, Any]): RuleInfo = RuleInfoGen(param)

//  protected def getName(param: Map[String, Any]) = param.getOrElse(_name, RuleStepNameGenerator.genName).toString
//  protected def getRule(param: Map[String, Any]) = param.getOrElse(_rule, "").toString
//  protected def getDetails(param: Map[String, Any]) = param.get(_details) match {
//    case Some(dt: Map[String, Any]) => dt
//    case _ => Map[String, Any]()
//  }

  def getPersistNames(steps: Seq[RuleStep]): Seq[String] = steps.map(_.ruleInfo.persistName)

  protected def genRuleStep(timeInfo: TimeInfo, param: Map[String, Any]): Seq[RuleStep]
  protected def adaptConcreteRuleStep(ruleStep: RuleStep, dsTmsts: Map[String, Set[Long]]): Seq[ConcreteRuleStep]
  def genConcreteRuleStep(timeInfo: TimeInfo, param: Map[String, Any], dsTmsts: Map[String, Set[Long]]
                         ): Seq[ConcreteRuleStep] = {
    genRuleStep(timeInfo, param).flatMap { rs =>
      adaptConcreteRuleStep(rs, dsTmsts)
    }
  }

}

object RuleInfoKeys {
  val _name = "name"
  val _rule = "rule"
  val _details = "details"

  val _dslType = "dsl.type"
  val _dqType = "dq.type"
}
import RuleInfoKeys._
import org.apache.griffin.measure.utils.ParamUtil._

object RuleInfoGen {
  def apply(param: Map[String, Any]): RuleInfo = {
    RuleInfo(
      param.getString(_name, RuleStepNameGenerator.genName),
      param.getString(_rule, ""),
      param.getParamMap(_details)
    )
  }

  def dslType(param: Map[String, Any]): DslType = DslType(param.getString(_dslType, ""))
  def dqType(param: Map[String, Any]): DqType = DqType(param.getString(_dqType, ""))
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