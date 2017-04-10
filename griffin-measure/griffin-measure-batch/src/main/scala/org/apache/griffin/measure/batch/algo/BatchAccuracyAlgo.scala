package org.apache.griffin.measure.batch.algo

import java.util.Date

import org.apache.griffin.measure.batch.config.params.AllParam
import org.apache.griffin.measure.batch.connector._
import org.apache.griffin.measure.batch.dsl.RuleAnalyzer
import org.apache.griffin.measure.batch.dsl.expr._
import org.apache.griffin.measure.batch.persist._
import org.apache.griffin.measure.batch.rule._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}


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

      // rules
      val ruleFactory = RuleFactory(userParam.evaluateRuleParam.assertionParam)
      val rule: StatementExpr = ruleFactory.generateRule()
      val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

      // data connector
      val sourceDataConnector: DataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, userParam.sourceParam.connector, ruleAnalyzer.sourceDataKeyExprs, ruleAnalyzer.sourceDataExprs) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("source data connection error!")
          }
          case Failure(ex) => throw new Exception(ex)
        }
      val targetDataConnector: DataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, userParam.targetParam.connector, ruleAnalyzer.targetDatakeyExprs, ruleAnalyzer.targetDataExprs) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("target data connection error!")
          }
          case Failure(ex) => throw new Exception(ex)
        }

      // get metadata
      val sourceMetaData: Iterable[(String, String)] = sourceDataConnector.metaData() match {
        case Success(md) => md
        case _ => throw new Exception("source metadata error!")
      }
      val targetMetaData: Iterable[(String, String)] = targetDataConnector.metaData() match {
        case Success(md) => md
        case _ => throw new Exception("target metadata error!")
      }

      // get data
      val sourceData: RDD[(Product, Map[String, Any])] = sourceDataConnector.data() match {
        case Success(dt) => dt
        case _ => throw new Exception("source data error!")
      }
      val targetData: RDD[(Product, Map[String, Any])] = targetDataConnector.data() match {
        case Success(dt) => dt
        case _ => throw new Exception("target data error!")
      }

      // fixme: accuracy algorithm

    }
  }

}
