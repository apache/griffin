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
          (false, Map[String, Any](MismatchInfo.wrap("no target")))
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

    // 1. merge source and target cached data
    val mergedData: Map[String, Any] = mergeData(source, target)

    // 2. check valid
    if (ruleAnalyzer.rule.valid(mergedData)) {
      // 3. substitute the cached data into statement, get the statement value
      // currently we can not get the mismatch reason, we need to add such information to figure out how it mismatches
      ((ruleAnalyzer.rule.calculate(mergedData) match {
        case Some(b: Boolean) => b
        case _ => false
      }), Map[String, Any](MismatchInfo.wrap("not matched"), TargetInfo.wrap(target._1)))
    } else {
      (false, Map[String, Any](MismatchInfo.wrap("invalid to compare"), TargetInfo.wrap(target._1)))
    }

  }

  private def mergeData(source: (V, T), target: (V, T)): Map[String, Any] = {
    source._1 ++ target._1
  }

}
