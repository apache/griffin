package org.apache.griffin

import org.apache.griffin.algo._
import org.apache.griffin.config.ConfigFileReader
import org.apache.griffin.config.params._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.JavaConverters._

object Application extends Logging {
//object Application {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logError("Usage: class [<config-files>]")
      sys.exit(-1)
    }

    val Array(configFile) = args

    // read config file
    val configReader = ConfigFileReader(configFile)
//    val allParam = configReader.readConfig[AllParam]
    val allParam = configReader.readConfig

    // spark config
    val sparkParam = allParam.sparkParam

    // type config
    val typeConfigParam = allParam.typeConfigParam
    val dqType = DqType.parse(typeConfigParam.dqType)
    val dataAssetTypeMap = typeConfigParam.dataAssetTypeMap

    // data assets
    val dataAssetParamMap = allParam.dataAssetParamMap

    // validation of params for algo
    val validation = dqType match {
      case AccuracyType => {
        (dataAssetTypeMap.contains("source") && dataAssetParamMap.contains("source")) &&
          (dataAssetTypeMap.contains("target") && dataAssetParamMap.contains("target"))
      }
      case _ => (dataAssetTypeMap.contains("source") && dataAssetParamMap.contains("source"))
    }
    if (!validation) {
      logError("Config Error!")
      sys.exit(-1)
    }

    // choose algo
//    val algo: Algo = dqType match {
//      case AccuracyType => {
//        dataAssetTypeMap.get("source") match {
//          case Some("kafka") => StreamingAccuracyAlgo(sparkParam, dataAssetParamMap.get("source").get, dataAssetParamMap.get("target").get)
//          case _ => StreamingAccuracyAlgo(sparkParam, dataAssetParamMap.get("source").get, dataAssetParamMap.get("target").get)
//        }
//      }
//      case _ => {
//        StreamingAccuracyAlgo(sparkParam, dataAssetParamMap.get("source").get, dataAssetParamMap.get("target").get)
//      }
//    }
    val algo: Algo = StreamingAccuracyAlgo4Crawler(allParam)

    algo.run

  }

}
