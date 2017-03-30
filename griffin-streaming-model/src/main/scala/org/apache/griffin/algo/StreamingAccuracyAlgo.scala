package org.apache.griffin.algo

import org.apache.griffin.config.params._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class StreamingAccuracyAlgo(val sparkParam: SparkParam,
                                 val sourceDataParam: DataAssetParam,
                                 val targetDataParam: DataAssetParam
                                ) extends AccuracyAlgo {

  def run(): Unit = {
    // create context
//    val sparkConf = new SparkConf().setAppName(sparkParam.appName)
//    sparkConf.setAll(sparkParam.config)
//
//    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel(sparkParam.logLevel)
//    val sqlContext = new HiveContext(sc)
//    val ssc = new StreamingContext(sc, Seconds(sparkParam.interval))
//    ssc.checkpoint(sparkParam.cpDir)


  }

}
