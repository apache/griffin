package org.apache.griffin.measure.batch.algo

import java.util.Date

import org.apache.griffin.measure.batch.algo.core.AccuracyCore
import org.apache.griffin.measure.batch.config.params.AllParam
import org.apache.griffin.measure.batch.connector._
import org.apache.griffin.measure.batch.rule._
import org.apache.griffin.measure.batch.rule.expr._
import org.apache.griffin.measure.batch.persist._
import org.apache.griffin.measure.batch.result._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

// accuracy algorithm for batch mode
case class BatchAccuracyAlgo(allParam: AllParam) extends AccuracyAlgo {
  val envParam = allParam.envParam
  val userParam = allParam.userParam

  def run(): Try[_] = {
    Try {
      val metricName = userParam.name

      val conf = new SparkConf().setAppName(metricName)
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)

      // start time
      val startTime = new Date().getTime()

      // get persists to persist measure result
      val persist: Persist = PersistFactory(envParam.persistParams, metricName).getPersists(startTime)

      // get spark application id
      val applicationId = sc.applicationId

      // persist start id
      persist.start(applicationId)

      // generate rule from rule param, generate rule analyzer
      val ruleFactory = RuleFactory(userParam.evaluateRuleParam)
      val rule: StatementExpr = ruleFactory.generateRule()
      val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

      // const expr value map
      val constExprValueMap = ExprValueUtil.genExprValueMap(None, ruleAnalyzer.constCacheExprs, Map[String, Any]())
      val finalConstExprValueMap = ExprValueUtil.updateExprValueMap(ruleAnalyzer.constFinalCacheExprs, constExprValueMap)

      // data connector
      val sourceDataConnector: DataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, userParam.sourceParam,
          ruleAnalyzer.sourceRuleExprs, finalConstExprValueMap
        ) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("source data connection error!")
          }
          case Failure(ex) => throw ex
        }
      val targetDataConnector: DataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, userParam.targetParam,
          ruleAnalyzer.targetRuleExprs, finalConstExprValueMap
        ) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("target data connection error!")
          }
          case Failure(ex) => throw ex
        }

      // get metadata
//      val sourceMetaData: Iterable[(String, String)] = sourceDataConnector.metaData() match {
//        case Success(md) => md
//        case _ => throw new Exception("source metadata error!")
//      }
//      val targetMetaData: Iterable[(String, String)] = targetDataConnector.metaData() match {
//        case Success(md) => md
//        case _ => throw new Exception("target metadata error!")
//      }

      // get data
      val sourceData: RDD[(Product, Map[String, Any])] = sourceDataConnector.data() match {
        case Success(dt) => dt
        case Failure(ex) => throw ex
      }
      val targetData: RDD[(Product, Map[String, Any])] = targetDataConnector.data() match {
        case Success(dt) => dt
        case Failure(ex) => throw ex
      }

      // accuracy algorithm
      val (accuResult, missingRdd, matchedRdd) = accuracy(sourceData, targetData, ruleAnalyzer)

      // end time
      val endTime = new Date().getTime
      persist.log(endTime, s"calculation using time: ${endTime - startTime} ms")

      // persist result
      persist.result(endTime, accuResult)
      val missingRecords = missingRdd.map(record2String(_, ruleAnalyzer.sourceRuleExprs.persistExprs, ruleAnalyzer.targetRuleExprs.persistExprs))
      persist.missRecords(missingRecords)

      // persist end time
      val persistEndTime = new Date().getTime
      persist.log(persistEndTime, s"persist using time: ${persistEndTime - endTime} ms")

      // finish
      persist.finish()

      // context stop
      sc.stop

    }
  }

  def wrapInitData(data: Map[String, Any]): (Map[String, Any], Map[String, Any]) = {
    (data, Map[String, Any]())
  }

  // calculate accuracy between source data and target data
  def accuracy(sourceData: RDD[(Product, Map[String, Any])], targetData: RDD[(Product, Map[String, Any])], ruleAnalyzer: RuleAnalyzer
              ): (AccuracyResult, RDD[(Product, (Map[String, Any], Map[String, Any]))], RDD[(Product, (Map[String, Any], Map[String, Any]))]) = {

    // 1. wrap data
    val sourceWrappedData: RDD[(Product, (Map[String, Any], Map[String, Any]))] = sourceData.map(r => (r._1, wrapInitData(r._2)))
    val targetWrappedData: RDD[(Product, (Map[String, Any], Map[String, Any]))] = targetData.map(r => (r._1, wrapInitData(r._2)))

    // 2. cogroup
    val allKvs = sourceWrappedData.cogroup(targetWrappedData)

    // 3. accuracy calculation
    val (accuResult, missingRdd, matchedRdd) = AccuracyCore.accuracy(allKvs, ruleAnalyzer)

    (accuResult, missingRdd, matchedRdd)
  }

  // convert data into a string
  def record2String(rec: (Product, (Map[String, Any], Map[String, Any])), sourcePersist: Iterable[Expr], targetPersist: Iterable[Expr]): String = {
    val (key, (data, info)) = rec
    val persistData = getPersistMap(data, sourcePersist)
    val persistInfo = info.mapValues { value =>
      value match {
        case vd: Map[String, Any] => getPersistMap(vd, targetPersist)
        case v => v
      }
    }
    s"${persistData} [${persistInfo}]"
  }

  // get the expr value map of the persist expressions
  private def getPersistMap(data: Map[String, Any], persist: Iterable[Expr]): Map[String, Any] = {
    val persistMap = persist.map(e => (e._id, e.desc)).toMap
    data.flatMap { pair =>
      val (k, v) = pair
      persistMap.get(k) match {
        case Some(d) => Some((d -> v))
        case _ => None
      }
    }
  }

}
