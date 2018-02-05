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

import org.apache.griffin.measure.process.temp._
import org.apache.griffin.measure.process._
import org.apache.griffin.measure.rule.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.rule.plan.{TimeInfo, _}
import org.apache.griffin.measure.rule.trans._

import scala.util.{Failure, Success}

case class GriffinDslAdaptor(dataSourceNames: Seq[String],
                             functionNames: Seq[String]
                            ) extends RuleAdaptor {

  import RuleParamKeys._

  val filteredFunctionNames = functionNames.filter { fn =>
    fn.matches("""^[a-zA-Z_]\w*$""")
  }
  val parser = GriffinDslParser(dataSourceNames, filteredFunctionNames)

  private val emptyRulePlan = RulePlan(Nil, Nil)

  override def genRulePlan(timeInfo: TimeInfo, param: Map[String, Any],
                           processType: ProcessType, dsTimeRanges: Map[String, TimeRange]
                          ): RulePlan = {
    val name = getRuleName(param)
    val rule = getRule(param)
    val dqType = getDqType(param)
    try {
      val result = parser.parseRule(rule, dqType)
      if (result.successful) {
        val expr = result.get
        val rulePlanTrans = RulePlanTrans(dqType, dataSourceNames, timeInfo,
          name, expr, param, processType, dsTimeRanges)
        rulePlanTrans.trans match {
          case Success(rp) => rp
          case Failure(ex) => {
            warn(s"translate rule [ ${rule} ] fails: \n${ex.getMessage}")
            emptyRulePlan
          }
        }
      } else {
        warn(s"parse rule [ ${rule} ] fails: \n${result}")
        emptyRulePlan
      }
    } catch {
      case e: Throwable => {
        error(s"generate rule plan ${name} fails: ${e.getMessage}")
        emptyRulePlan
      }
    }
  }

}
