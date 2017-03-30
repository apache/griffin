package org.apache.griffin.algo.core

import org.apache.griffin.dump._
import org.apache.griffin.record.result.AccuResult
import org.apache.spark.rdd.RDD


object AccuracyCore {

  type T = Map[String, Any]

  def accuracy(allkvs: RDD[(Product, (Iterable[(T, T)], Iterable[(T, T)]))]
              ): (AccuResult, RDD[(Product, (T, T))], RDD[(Product, (T, T))]) = {
    val result: RDD[(Long, Long, List[(Product, (T, T))], List[(Product, (T, T))])] = allkvs.map { kv =>
      val (k, (srcData, tgtData)) = kv
//      val srcCount = srcData.size
      val tgtCount = tgtData.size
      val tgt: Set[T] = tgtData.map(_._1).toSet   // only get value

      // result: (missCount, matchCount, missDataList, matchDataList)
      val rslt = srcData.foldLeft((0L, 0L, List[(Product, (T, T))](), List[(Product, (T, T))]())) { (r, mapPair) =>
        val (valueMap, infoMap) = mapPair
        if (tgt.contains(valueMap)) {
          val newInfoMap = infoMap + (MismatchInfo.key -> "matched")
          val matchItem = (k, (valueMap, newInfoMap))
          (r._1, r._2 + 1, r._3, r._4 :+ matchItem)
        } else {
          val newInfoMap = if (tgtCount == 0) {
            infoMap + (MismatchInfo.key -> "no target")
          } else if (tgtCount == 1) {
            infoMap + (MismatchInfo.key -> ("target data: " + tgt.head.toString))
          } else {
            infoMap + (MismatchInfo.key -> "not match")
          }
          val missItem = (k, (valueMap, newInfoMap))
          (r._1 + 1, r._2, r._3 :+ missItem, r._4)
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

    (AccuResult(countPair._1, (countPair._1 + countPair._2)), missRdd, matchRdd)
  }

//  def accuracy(allkvs: RDD[(Product, (Iterable[Map[String, Any]], Iterable[Map[String, Any]]))]): (AccuResult, List[String], RDD[(Product, Map[String, Any])]) = {
//    val result: RDD[((Long, Long), List[String], List[(Product, Map[String, Any])])] = allkvs.map { kv =>
//      val (k, (srcData, tgtData)) = kv
//      val srcCount = srcData.size
//      val tgtCount = tgtData.size
//      val tgt = tgtData.toSet
//      val rslt = srcData.foldLeft((0L, List[String](), List[(Product, Map[String, Any])]())) { (r, mp) =>
//        if (tgt.contains(mp)) {
//          r
//        } else {
//          val mp1 = if (tgtCount == 0) {
//            mp + ("_error_" -> "no target")
//          } else if (tgtCount == 1) {
//            mp + ("_error_" -> tgt.head.toString)
//          } else {
//            mp + ("_error_" -> "not match")
//          }
//          (r._1 + 1, mp1.toString :: r._2, (k, mp) :: r._3)
//        }
//      }
//      ((rslt._1, srcCount.toLong), rslt._2, rslt._3)
//    }
//
//    val missingRdd = result.flatMap(_._3)
//
//    def seq(cnt: (Long, Long, List[String]), rcd: ((Long, Long), List[String], Any)): (Long, Long, List[String]) = {
//      (cnt._1 + rcd._1._1, cnt._2 + rcd._1._2, cnt._3 ::: rcd._2)
//    }
//    def comb(c1: (Long, Long, List[String]), c2: (Long, Long, List[String])): (Long, Long, List[String]) = {
//      (c1._1 + c2._1, c1._2 + c2._2, c1._3 ::: c2._3)
//    }
//    val missingCount = result.aggregate((0L, 0L, List[String]()))(seq, comb)
//
//    (AccuResult(missingCount._1, missingCount._2), missingCount._3, missingRdd)
//  }

}
