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
package org.apache.griffin.measure.step.builder.preproc

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam

/**
  * generate rule params by template defined in pre-proc param
  */
object PreProcRuleParamGenerator {

  case class StringAnyMap(values:Map[String,Any])

  val _name = "name"

  def getNewPreProcRules(rules: Seq[RuleParam], suffix: String): Seq[RuleParam] = {
    rules.map { rule =>
      getNewPreProcRule(rule, suffix)
    }
  }

  private def getNewPreProcRule(param: RuleParam, suffix: String): RuleParam = {
    val newName = genNewString(param.getName, suffix)
    val newRule = genNewString(param.getRule, suffix)
    val newDetails = getNewMap(param.getDetails, suffix)
    param.replaceName(newName).replaceRule(newRule).replaceDetails(newDetails)
  }

  private def getNewMap(details: Map[String, Any], suffix: String): Map[String, Any] = {
    val keys = details.keys
    keys.foldLeft(details) { (map, key) =>
      map.get(key) match {
        case Some(s: String) => map + (key -> genNewString(s, suffix))
        case Some(subMap: StringAnyMap) => map + (key -> getNewMap(subMap.values, suffix))
        case Some(arr: Seq[_]) => map + (key -> getNewArr(arr, suffix))
        case _ => map
      }
    }
  }

  private def getNewArr(paramArr: Seq[Any], suffix: String): Seq[Any] = {
    paramArr.foldLeft(Nil: Seq[Any]) { (res, param) =>
      param match {
        case s: String => res :+ genNewString(s, suffix)
        case map: StringAnyMap => res :+ getNewMap(map.values, suffix)
        case arr: Seq[_] => res :+ getNewArr(arr, suffix)
        case _ => res :+ param
      }
    }
  }

  private def genNewString(str: String, suffix: String): String = {
    str.replaceAll("""\$\{(.*)\}""", s"$$1_${suffix}")
  }

}
