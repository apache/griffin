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
package org.apache.griffin.measure.rule.trans

import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.ProcessType
import org.apache.griffin.measure.process.temp.TimeRange
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.dsl.expr.Expr
import org.apache.griffin.measure.rule.plan._

import scala.util.Try

trait RulePlanTrans extends Loggable with Serializable {

  protected val emptyRulePlan = RulePlan(Nil, Nil)
  protected val emptyMap = Map[String, Any]()

  def trans(): Try[RulePlan]

}

object RulePlanTrans {
  private val emptyRulePlanTrans = new RulePlanTrans {
    def trans(): Try[RulePlan] = Try(emptyRulePlan)
  }

  def apply(dqType: DqType,
            dsNames: Seq[String],
            ti: TimeInfo, name: String, expr: Expr,
            param: Map[String, Any], procType: ProcessType,
            dsTimeRanges: Map[String, TimeRange]
           ): RulePlanTrans = {
    dqType match {
      case AccuracyType => AccuracyRulePlanTrans(dsNames, ti, name, expr, param, procType)
      case ProfilingType => ProfilingRulePlanTrans(dsNames, ti, name, expr, param, procType)
      case UniquenessType => UniquenessRulePlanTrans(dsNames, ti, name, expr, param, procType)
      case DistinctnessType => DistinctnessRulePlanTrans(dsNames, ti, name, expr, param, procType, dsTimeRanges)
      case TimelinessType => TimelinessRulePlanTrans(dsNames, ti, name, expr, param, procType, dsTimeRanges)
      case CompletenessType => CompletenessRulePlanTrans(dsNames, ti, name, expr, param, procType)
      case _ => emptyRulePlanTrans
    }
  }
}
