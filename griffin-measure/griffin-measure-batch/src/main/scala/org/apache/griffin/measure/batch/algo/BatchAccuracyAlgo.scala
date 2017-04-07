package org.apache.griffin.measure.batch.algo

import java.util.Date

import org.apache.griffin.measure.batch.config.params.AllParam
import org.apache.griffin.measure.batch.connector._
import org.apache.griffin.measure.batch.dsl.expr._
import org.apache.griffin.measure.batch.persist._
import org.apache.griffin.measure.batch.rule._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Success, Try}


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
      val rules: Iterable[StatementExpr] = ruleFactory.generateRules()

      // get data related expressions
//      val sourceDataExprs =


      // data connector
      val sourceDataConnector: DataConnector = HiveDataConnector(sqlContext, userParam.sourceParam.connector.config)
      val targetDataConnector: DataConnector = HiveDataConnector(sqlContext, userParam.targetParam.connector.config)

      if (!sourceDataConnector.available) {
        throw new Exception("source data connection error!")
      }
      if (!targetDataConnector.available) {
        throw new Exception("target data connection error!")
      }

      // get metadata
      val sourceMetaData: Iterable[(String, String)] = sourceDataConnector.metaData() match {
        case Success(md) => md
        case _ => throw new Exception("source metadata error!")
      }
      val targetMetaData: Iterable[(String, String)] = targetDataConnector.metaData() match {
        case Success(md) => md
        case _ => throw new Exception("source metadata error!")
      }

      // get data
      ;

    }
  }

}
