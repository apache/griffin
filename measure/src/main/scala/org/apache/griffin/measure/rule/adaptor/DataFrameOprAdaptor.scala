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

import org.apache.griffin.measure.process.{ExportMode, ProcessType}
import org.apache.griffin.measure.process.temp.TimeRange
import org.apache.griffin.measure.rule.plan.{TimeInfo, _}
import org.apache.griffin.measure.utils.ParamUtil._

case class DataFrameOprAdaptor() extends RuleAdaptor {

//  def genRuleStep(timeInfo: TimeInfo, param: Map[String, Any]): Seq[RuleStep] = {
//    val ruleInfo = RuleInfoGen(param, timeInfo)
//    DfOprStep(timeInfo, ruleInfo) :: Nil
////    DfOprStep(getName(param), getRule(param), getDetails(param),
////      getPersistType(param), getUpdateDataSource(param)) :: Nil
//  }
//  def adaptConcreteRuleStep(ruleStep: RuleStep): Seq[ConcreteRuleStep] = {
//    ruleStep match {
//      case rs @ DfOprStep(_, _) => rs :: Nil
//      case _ => Nil
//    }
//  }

//  def getTempSourceNames(param: Map[String, Any]): Seq[String] = {
//    param.get(_name) match {
//      case Some(name) => name.toString :: Nil
//      case _ => Nil
//    }
//  }

  import RuleParamKeys._

  def genRulePlan(timeInfo: TimeInfo, param: Map[String, Any],
                  procType: ProcessType, dsTimeRanges: Map[String, TimeRange]): RulePlan = {
    val name = getRuleName(param)
    val step = DfOprStep(name, getRule(param), getDetails(param), getCache(param), getGlobal(param))
    val mode = ExportMode.defaultMode(procType)
    RulePlan(step :: Nil, genRuleExports(param, name, name, timeInfo.calcTime, mode))
  }

}
