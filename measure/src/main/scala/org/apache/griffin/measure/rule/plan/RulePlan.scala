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
package org.apache.griffin.measure.rule.plan

import scala.reflect.ClassTag

case class RulePlan(ruleSteps: Seq[RuleStep],
                    ruleExports: Seq[RuleExport],
                    dsUpdates: Seq[DsUpdate] = Nil
                   ) extends Serializable {

  val globalRuleSteps = filterRuleSteps(_.global)
  val normalRuleSteps = filterRuleSteps(!_.global)

  val metricExports = filterRuleExports[MetricExport](ruleExports)
  val recordExports = filterRuleExports[RecordExport](ruleExports)

  private def filterRuleSteps(func: (RuleStep) => Boolean): Seq[RuleStep] = {
    ruleSteps.filter(func)
  }

  private def filterRuleExports[T <: RuleExport: ClassTag](exports: Seq[RuleExport]): Seq[T] = {
    exports.flatMap { exp =>
      exp match {
        case e: T => Some(e)
        case _ => None
      }
    }
  }

//  def ruleStepNames(func: (RuleStep) => Boolean): Seq[String] = {
//    ruleSteps.filter(func).map(_.name)
//  }

  def merge(rp: RulePlan): RulePlan = {
    RulePlan(
      this.ruleSteps ++ rp.ruleSteps,
      this.ruleExports ++ rp.ruleExports,
      this.dsUpdates ++ rp.dsUpdates
    )
  }

}
