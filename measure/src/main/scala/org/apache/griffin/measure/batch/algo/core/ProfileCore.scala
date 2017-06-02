/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.batch.algo.core

import org.apache.griffin.measure.batch.rule.RuleAnalyzer
import org.apache.griffin.measure.batch.result._
import org.apache.spark.rdd.RDD


object ProfileCore {

  type V = Map[String, Any]
  type T = Map[String, Any]

  // dataRdd: rdd of (key, (sourceData, sourceInfo))
  // output: accuracy result, missing source data rdd, matched source data rdd
  def profile(dataRdd: RDD[(Product, (V, T))], ruleAnalyzer: RuleAnalyzer
              ): (ProfileResult, RDD[(Product, (V, T))], RDD[(Product, (V, T))]) = {

    val resultRdd: RDD[((Product, (V, T)), Boolean)] = dataRdd.map { kv =>
      val (key, (data, info)) = kv
      val (matched, missInfo) = matchData((data, info), ruleAnalyzer)
      ((key, (data, info ++ missInfo)), matched)
    }

    val totalCount = resultRdd.count
    val matchRdd = resultRdd.filter(_._2).map(_._1)
    val matchCount = matchRdd.count
    val missRdd = resultRdd.filter(!_._2).map(_._1)
    val missCount = missRdd.count

    (ProfileResult(matchCount, totalCount), missRdd, matchRdd)

  }

  // try to match data as rule, return true if matched, false if unmatched
  private def matchData(dataPair: (V, T), ruleAnalyzer: RuleAnalyzer): (Boolean, T) = {

    val data: Map[String, Any] = dataPair._1

    // 1. check valid
    if (ruleAnalyzer.rule.valid(data)) {
      // 2. substitute the cached data into statement, get the statement value
      val matched = ruleAnalyzer.rule.calculate(data) match {
        case Some(b: Boolean) => b
        case _ => false
      }
      // currently we can not get the mismatch reason, we need to add such information to figure out how it mismatches
      if (matched) (matched, Map[String, Any]())
      else (matched, Map[String, Any](MismatchInfo.wrap("not matched")))
    } else {
      (false, Map[String, Any](MismatchInfo.wrap("invalid to compare")))
    }

  }

}
