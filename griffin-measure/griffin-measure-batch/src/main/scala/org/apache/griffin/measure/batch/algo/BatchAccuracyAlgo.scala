package org.apache.griffin.measure.batch.algo

import java.util.Date

import org.apache.griffin.measure.batch.config.params.AllParam
import org.apache.griffin.measure.batch.persist._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try


case class BatchAccuracyAlgo(allParam: AllParam) extends AccuracyAlgo {
  val envParam = allParam.envParam
  val userParam = allParam.userParam

  def run(): Try[_] = {
    Try {
      val metricName = userParam.name

      val conf = new SparkConf().setAppName(metricName)
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)

      // current time
      val curTime = new Date().getTime()

      // get persists
      val persist: Persist = PersistFactory(envParam.persistParams, metricName).getPersists(curTime)

      // get spark application id
      val applicationId = sc.applicationId

      // start
      persist.start(applicationId)

      // fixme: data connector
    }
  }

}
