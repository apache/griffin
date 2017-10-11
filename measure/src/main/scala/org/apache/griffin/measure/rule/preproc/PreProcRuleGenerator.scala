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
package org.apache.griffin.measure.rule.preproc

object PreProcRuleGenerator {

  val _name = "name"

  def genPreProcRules(rules: Seq[Map[String, Any]], suffix: String): Seq[Map[String, Any]] = {
    if (rules == null) Nil else {
      rules.map { rule =>
        genPreProcRule(rule, suffix)
      }
    }
  }

  def getRuleNames(rules: Seq[Map[String, Any]]): Seq[String] = {
    if (rules == null) Nil else {
      rules.flatMap { rule =>
        rule.get(_name) match {
          case Some(s: String) => Some(s)
          case _ => None
        }
      }
    }
  }

  private def genPreProcRule(param: Map[String, Any], suffix: String
                            ): Map[String, Any] = {
    val keys = param.keys
    keys.foldLeft(param) { (map, key) =>
      map.get(key) match {
        case Some(s: String) => map + (key -> genNewString(s, suffix))
        case Some(subMap: Map[String, Any]) => map + (key -> genPreProcRule(subMap, suffix))
        case Some(arr: Seq[_]) => map + (key -> genPreProcRule(arr, suffix))
        case _ => map
      }
    }
  }

  private def genPreProcRule(paramArr: Seq[Any], suffix: String): Seq[Any] = {
    paramArr.foldLeft(Nil: Seq[Any]) { (res, param) =>
      param match {
        case s: String => res :+ genNewString(s, suffix)
        case map: Map[String, Any] => res :+ genPreProcRule(map, suffix)
        case arr: Seq[_] => res :+ genPreProcRule(arr, suffix)
        case _ => res :+ param
      }
    }
  }

  private def genNewString(str: String, suffix: String): String = {
    str.replaceAll("""\$\{(.*)\}""", s"$$1_${suffix}")
  }

}
