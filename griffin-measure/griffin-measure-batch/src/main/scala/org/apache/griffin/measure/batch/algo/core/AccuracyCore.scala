package org.apache.griffin.measure.batch.algo.core

import org.apache.griffin.measure.batch.rule.RuleAnalyzer
import org.apache.griffin.measure.batch.result._
import org.apache.spark.rdd.RDD


object AccuracyCore {

  type V = Map[String, Any]
  type T = Map[String, Any]

  def accuracy(allKvs: RDD[(Product, (Iterable[(V, T)], Iterable[(V, T)]))], ruleAnalyzer: RuleAnalyzer
              ): (AccuracyResult, RDD[(Product, (V, T))], RDD[(Product, (V, T))]) = {
    val result: RDD[(Long, Long, List[(Product, (V, T))], List[(Product, (V, T))])] = allKvs.map { kv =>
      val (key, (sourceDatas, targetDatas)) = kv

      // result: (missCount, matchCount, missDataList, matchDataList)
      val rslt = sourceDatas.foldLeft((0L, 0L, List[(Product, (V, T))](), List[(Product, (V, T))]())) { (sr, sourcePair) =>
        val matchResult = if (targetDatas.isEmpty) {
          (false, Map[String, Any]((MismatchInfo.key -> "no target")))
        } else {
          targetDatas.foldLeft((false, Map[String, Any]())) { (tr, targetPair) =>
            if (tr._1) tr
            else matchData(sourcePair, targetPair, ruleAnalyzer)
          }
        }

        if (matchResult._1) {
          val matchItem = (key, sourcePair)
          (sr._1, sr._2 + 1, sr._3, sr._4 :+ matchItem)
        } else {
          val missItem = (key, (sourcePair._1, sourcePair._2 ++ matchResult._2))
          (sr._1 + 1, sr._2, sr._3 :+ missItem, sr._4)
        }
      }

      rslt
    }

    val missRdd = result.flatMap(_._3)
    val matchRdd = result.flatMap(_._4)

    def seq(cnt: (Long, Long), rcd: (Long, Long, Any, Any)): (Long, Long) = {
      (cnt._1 + rcd._1, cnt._2 + rcd._2)
    }
    def comb(c1: (Long, Long), c2: (Long, Long)): (Long, Long) = {
      (c1._1 + c2._1, c1._2 + c2._2)
    }
    val countPair = result.aggregate((0L, 0L))(seq, comb)

    (AccuracyResult(countPair._1, (countPair._1 + countPair._2)), missRdd, matchRdd)
  }

  private def matchData(source: (V, T), target: (V, T), ruleAnalyzer: RuleAnalyzer): (Boolean, T) = {

    // 1. merge source and target data
    val mergedData: Map[String, Any] = mergeData(source, target)

    // 2. get assign variables by substituting mergedData, and merge into data map
    val dataMap: Map[String, Any] = ruleAnalyzer.assigns.foldLeft(mergedData) { (dataMap, assign) =>
      assign.right.genValue(dataMap) match {
        case Some(v) => dataMap + (assign.left.name -> v)
        case _ => dataMap
      }
    }

    // 3. condition judgement by substituting from data map
    val conditionPass = ruleAnalyzer.conditions.foldLeft(true) { (pass, condition) =>
      pass && (condition.genValue(dataMap) match {
        case Some(b) => b
        case _ => false
      })
    }

    if (conditionPass) {
      // 4. mapping calculation by substituting from data map
      ruleAnalyzer.mappings.foldLeft((true, Map[String, Any]())) { (res, mapping) =>
        val (matched, info) = res
        if (!matched) res
        else {
          ((mapping.genValue(dataMap) match {
            case Some(b) => b
            case _ => false
          }), info + (MismatchInfo.key -> s"not matched with ${target._1}"))
        }
      }
    } else {
      (false, Map[String, Any]((MismatchInfo.key -> "condition fail")))
    }

  }

  private def mergeData(source: (V, T), target: (V, T)): Map[String, Any] = {
    source._1 ++ target._1
  }

}
