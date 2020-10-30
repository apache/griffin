/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.step.builder.preproc

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.configuration.enums.DslType._

/**
 * generate each entity pre-proc params by template defined in pre-proc param
 */
object PreProcParamMaker {

  case class StringAnyMap(values: Map[String, Any])

  def makePreProcRules(
      rules: Seq[RuleParam],
      suffix: String,
      dfName: String): (Seq[RuleParam], String) = {
    val len = rules.size
    val (newRules, _) = rules.zipWithIndex.foldLeft((Nil: Seq[RuleParam], dfName)) {
      (ret, pair) =>
        val (rls, prevOutDfName) = ret
        val (rule, i) = pair
        val inName = rule.getInDfName(prevOutDfName)
        val outName = if (i == len - 1) dfName else rule.getOutDfName(genNameWithIndex(dfName, i))
        val ruleWithNames = rule.replaceInOutDfName(inName, outName)
        (rls :+ makeNewPreProcRule(ruleWithNames, suffix), outName)
    }
    (newRules, withSuffix(dfName, suffix))
  }

  private def makeNewPreProcRule(rule: RuleParam, suffix: String): RuleParam = {
    val newInDfName = withSuffix(rule.getInDfName(), suffix)
    val newOutDfName = withSuffix(rule.getOutDfName(), suffix)
    val rpRule = rule.replaceInOutDfName(newInDfName, newOutDfName)
    rule.getDslType match {
      case DataFrameOpsType => rpRule
      case _ =>
        val newRule = replaceDfNameSuffix(rule.getRule, rule.getInDfName(), suffix)
        rpRule.replaceRule(newRule)
    }
  }

  private def genNameWithIndex(name: String, i: Int): String = s"$name$i"

  private def replaceDfNameSuffix(str: String, dfName: String, suffix: String): String = {
    val regexStr = s"(?i)$dfName"
    val replaceDfName = withSuffix(dfName, suffix)
    str.replaceAll(regexStr, replaceDfName)
  }

  def withSuffix(str: String, suffix: String): String = s"${str}_$suffix"

}
