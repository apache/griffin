package org.apache.griffin.cache

import org.apache.spark.rdd.RDD
import org.apache.griffin.record.result.AccuResult

case class CachedAccuData() extends CachedData {

  type T = AccuResult

//  var validMinRange: (Long, Long) = _

//  var proc: (Long) => (T, RDD[(Product, Map[String, Any])], (Long, Long)) => (T, RDD[(Product, Map[String, Any])], (Long, Long)) = _

//  def recalcAndCache(t: Long): Unit = {
//    val res = proc(t)(result, rdd, validMinRange)
//    unpersist
//    result = res._1
//    rdd = res._2
//    validMinRange = res._3
//    persist
//  }

}
