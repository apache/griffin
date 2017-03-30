package org.apache.griffin.cache

import org.apache.spark.rdd.RDD
import org.apache.griffin.record.result.Result


trait CachedData {

  type T <: Result

  var result: T = _

  var curTime: Long = _
  var deadTime: Long = _
//  var rdd: RDD[(Product, Map[String, Any])] = _

//  var nextFireTime: Long = _

//  def persist(): Unit = { rdd.cache }
//  def unpersist(): Unit = { rdd.unpersist() }

//  def recalcAndCache(t: Long): Unit
  def cacheEnd(): Boolean = {
    (curTime > deadTime) || (result != null && !result.needCalc)
  }

}
