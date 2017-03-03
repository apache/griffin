/**
  * Accuracy of source data comparing with target data
  *
  * Purpose: suppose that each row of source data could be found in target data,
  * but there exists some errors resulting the data missing, in this progress
  * we count the missing data of source data set, which is not found in the target data set
  *
  *
  *
  */

package org.apache.griffin.accuracy

import org.apache.griffin.dataLoaderUtils.DataLoaderFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.griffin.util.{HdfsUtils, PartitionUtils}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.{MutableList, HashSet => MutableHashSet, Map => MutableMap}

object Accu extends Logging{

  def main(args: Array[String]) {
    if (args.length < 2) {
      logError("Usage: class <input-conf-file> <outputPath>")
      logError("For input-conf-file, please use accu_config.json as an template to reflect test dataset accordingly.")
      sys.exit(-1)
    }
    val input = HdfsUtils.openFile(args(0))

    val outputPath = args(1) + System.getProperty("file.separator")

    //some done files, some are for job scheduling purpose
    val startFile = outputPath + "_START"
    val resultFile = outputPath + "_RESULT"
    val doneFile = outputPath + "_FINISHED"
    val missingFile = outputPath + "missingRec.txt"

    //deserialize json to bean object
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    //read the config info of comparison
    val configure = mapper.readValue(input, classOf[AccuracyConfEntity])

    val conf = new SparkConf().setAppName("Accu")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //add spark applicationId for debugging
    val applicationId = sc.applicationId

    //for spark monitoring
    HdfsUtils.writeFile(startFile, applicationId)

    //get source data and target data
    val dataLoader = DataLoaderFactory.getDataLoader(sqlContext, DataLoaderFactory.hive)
    val (sojdf, bedf) = dataLoader.getAccuDataFrame(configure)

    //-- algorithm --
    val ((missCount, srcCount), missedList) = calcAccu(configure, sojdf, bedf)

    //record result and notify done
    HdfsUtils.writeFile(resultFile, ((1 - missCount.toDouble / srcCount) * 100).toString())

    val sb = new StringBuilder
    missedList.foreach { item =>
      sb.append(item)
      sb.append("\n")
    }

    //for spark monitoring
    HdfsUtils.writeFile(missingFile, sb.toString())
    HdfsUtils.createFile(doneFile)

    sc.stop()

  }

  def calcAccu(configure: AccuracyConfEntity, sojdf: DataFrame, bedf: DataFrame): ((Long, Long), List[String]) = {
    val mp = configure.accuracyMapping

    //--0. prepare to start job--

    //the key column info, to match different rows between source and target
    val sojKeyIndexList = MutableList[Tuple2[Int, String]]()
    val beKeyIndexList = MutableList[Tuple2[Int, String]]()

    //the value column info, to be compared with between the match rows
    val sojValueIndexList = MutableList[Tuple2[Int, String]]()
    val beValueIndexList = MutableList[Tuple2[Int, String]]()

    //get the key and value column info from config
    for (i <- mp) {
      if (i.isPK) {

        val sojkey = Tuple2(i.sourceColId, i.sourceColName)
        sojKeyIndexList += sojkey

        val bekey = Tuple2(i.targetColId, i.targetColName)
        beKeyIndexList += bekey

      }

      val sojValue = Tuple2(i.sourceColId, i.sourceColName)
      sojValueIndexList += sojValue

      val beValue = Tuple2(i.targetColId, i.sourceColName)
      beValueIndexList += beValue

    }

    def toTuple[A <: Object](as: MutableList[A]): Product = {
      val tupleClass = Class.forName("scala.Tuple" + as.size)
      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
    }

    //--1. convert data into same format (key, value)--

    //convert source data rows into (key, ("__source__", valMap)), key is the key column value tuple, valMap is the value map of row
    val sojkvs = sojdf.map { row =>
      val kk = sojKeyIndexList map { t => row.get(t._1).asInstanceOf[Object] }

      val kkk = toTuple(kk)

      val len = row.length
      val v = sojValueIndexList.foldLeft(Map[String, Any]()) { (c, x) => c + (x._2 -> row.get(x._1)) }

      (kkk -> v)
    }

    //convert source data rows into (key, ("__target__", valMap)), key is the key column value tuple, valMap is the value map of row
    val bekvs = bedf.map { row =>
      val kk = beKeyIndexList map { t => row.get(t._1).asInstanceOf[Object] }

      val kkk = toTuple(kk)

      val len = row.length
      val v = beValueIndexList.foldLeft(Map[String, Any]()) { (c, x) => c + (x._2 -> row.get(x._1)) }

      (kkk -> v)
    }

    //--2. cogroup src RDD[(k, v1)] and tgt RDD[(k, v2)] into RDD[(k, (Iterable[v1], Iterable[v2]))]
    val allkvs = sojkvs.cogroup(bekvs)

    //--3. get missed count of source data--

    //with the same key, for each source data in list, if it does not exists in the target set, one missed data found
    def seqMissed(cnt: ((Long, Long), List[String]), kv: (Product, (Iterable[Map[String, Any]], Iterable[Map[String, Any]]))) = {
      val ls = kv._2._1
      val st = kv._2._2

      if (ls.size > 2 && st.size > 4) {
        val st1 = st.foldLeft(HashSet[Map[String, Any]]())((set, mp) => set + mp)
        val ss = ls.foldLeft((0, List[String]())) { (c, mp) =>
          if (st1.contains(mp)) {
            c
          } else {
            (c._1 + 1, mp.toString :: c._2)
          }
        }
        ((cnt._1._1 + ss._1, cnt._1._2 + ls.size), ss._2 ::: cnt._2)
      } else {
        val ss = ls.foldLeft((0, List[String]())) { (c, mp) =>
          if (st.exists(mp.equals(_))) {
            c
          } else {
            (c._1 + 1, mp.toString :: c._2)
          }
        }
        ((cnt._1._1 + ss._1, cnt._1._2 + ls.size), ss._2 ::: cnt._2)
      }
    }

    //add missed count of each partition
    def combMissed(cnt1: ((Long, Long), List[String]), cnt2: ((Long, Long), List[String])) = {
      ((cnt1._1._1 + cnt2._1._1, cnt1._1._2 + cnt2._1._2), cnt1._2 ::: cnt2._2)
    }

    //count missed source data
    val missed = allkvs.aggregate(((0L, 0L), List[String]()))(seqMissed, combMissed)

    //output: need to change
    logInfo("source count: " + missed._1._2 + " missed count : " + missed._1._1)

    missed
  }

}
