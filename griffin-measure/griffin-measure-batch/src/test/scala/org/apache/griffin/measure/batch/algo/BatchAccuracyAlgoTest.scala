package org.apache.griffin.measure.batch.algo

import java.util.Date

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.config.params.env._
import org.apache.griffin.measure.batch.config.params._
import org.apache.griffin.measure.batch.config.reader._
import org.apache.griffin.measure.batch.config.validator._
import org.apache.griffin.measure.batch.connector.{DataConnector, DataConnectorFactory, CacheDataUtil}
import org.apache.griffin.measure.batch.rule.{RuleAnalyzer, RuleFactory}
import org.apache.griffin.measure.batch.rule.expr._

import scala.util.{Failure, Success, Try}
import org.apache.griffin.measure.batch.log.Loggable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


@RunWith(classOf[JUnitRunner])
class BatchAccuracyAlgoTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {

  val envFile = "src/test/resources/env.json"
  val confFile = "src/test/resources/config.json"
  val fsType = "local"

  val args = Array(envFile, confFile)

  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  var allParam: AllParam = _

  before {
    // read param files
    val envParam = readParamFile[EnvParam](envFile, fsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val userParam = readParamFile[UserParam](confFile, fsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    allParam = AllParam(envParam, userParam)

    // validate param files
    validateParams(allParam) match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-3)
      }
      case _ => {
        info("params validation pass")
      }
    }

    val metricName = userParam.name
    val conf = new SparkConf().setMaster("local[*]").setAppName(metricName)
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  test("algorithm") {
    Try {
      val envParam = allParam.envParam
      val userParam = allParam.userParam

      // start time
      val startTime = new Date().getTime()

      // get spark application id
      val applicationId = sc.applicationId

      // rules
      val ruleFactory = RuleFactory(userParam.evaluateRuleParam)
      val rule: StatementExpr = ruleFactory.generateRule()
      val ruleAnalyzer: RuleAnalyzer = RuleAnalyzer(rule)

      ruleAnalyzer.globalCacheExprs.foreach(println)
      ruleAnalyzer.globalFinalCacheExprs.foreach(println)

      // global cache data
      val globalCachedData = CacheDataUtil.genCachedMap(None, ruleAnalyzer.globalCacheExprs, Map[String, Any]())
      val globalFinalCachedData = CacheDataUtil.filterCachedMap(ruleAnalyzer.globalFinalCacheExprs, globalCachedData)

      // data connector
      val sourceDataConnector: DataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, userParam.sourceParam,
          ruleAnalyzer.sourceGroupbyExprs, ruleAnalyzer.sourceCacheExprs,
          ruleAnalyzer.sourceFinalCacheExprs, globalFinalCachedData) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("source data not available!")
          }
          case Failure(ex) => throw ex
        }
      val targetDataConnector: DataConnector =
        DataConnectorFactory.getDataConnector(sqlContext, userParam.targetParam,
          ruleAnalyzer.targetGroupbyExprs, ruleAnalyzer.targetCacheExprs,
          ruleAnalyzer.targetFinalCacheExprs, globalFinalCachedData) match {
          case Success(cntr) => {
            if (cntr.available) cntr
            else throw new Exception("target data not available!")
          }
          case Failure(ex) => throw ex
        }

      // get metadata
//      val sourceMetaData: Iterable[(String, String)] = sourceDataConnector.metaData() match {
//        case Success(md) => md
//        case Failure(ex) => throw ex
//      }
//      val targetMetaData: Iterable[(String, String)] = targetDataConnector.metaData() match {
//        case Success(md) => md
//        case Failure(ex) => throw ex
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

//      println("-- global cache exprs --")
//      ruleAnalyzer.globalCacheExprs.foreach(a => println(s"${a._id} ${a.desc}"))
//      println("-- global cache data --")
//      globalCachedData.foreach(println)
//      println("-- global final cache data --")
//      globalFinalCachedData.foreach(println)
//
//      println("-- source persist exprs --")
//      ruleAnalyzer.sourcePersistExprs.foreach(a => println(s"${a._id} ${a.desc}"))
//      println("-- source cache exprs --")
//      ruleAnalyzer.sourceCacheExprs.foreach(a => println(s"${a._id} ${a.desc}"))
//
//      println("-- source --")
//      sourceData.foreach { a =>
//        val printMap = ruleAnalyzer.sourcePersistExprs.flatMap { expr =>
//          a._2.get(expr._id) match {
//            case Some(v) => Some((expr._id + expr.desc, v))
//            case _ => None
//          }
//        }.toMap
//        println(printMap)
//
//        val cacheMap = ruleAnalyzer.sourceCacheExprs.flatMap { expr =>
//          a._2.get(expr._id) match {
//            case Some(v) => Some((expr._id + expr.desc, v))
//            case _ => None
//          }
//        }.toMap
//        println(cacheMap)
//
//        println(a)
//        println(a._2.size)
//      }

//      println("-- target --")
//      targetData.foreach { a =>
//        val printMap = ruleAnalyzer.targetPersistExprs.flatMap { expr =>
//          a._2.get(expr._id) match {
//            case Some(v) => Some((expr.desc, v))
//            case _ => None
//          }
//        }.toMap
//        println(printMap)
//      }

      // my algo
      val algo = BatchAccuracyAlgo(allParam)

      // accuracy algorithm
      val (accuResult, missingRdd, matchingRdd) = algo.accuracy(sourceData, targetData, ruleAnalyzer)

      println(s"match percentage: ${accuResult.matchPercentage}, total count: ${accuResult.total}")

      missingRdd.foreach(println)

      // end time
      val endTime = new Date().getTime
      println(s"using time: ${endTime - startTime} ms")
    } match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-4)
      }
      case _ => {
        info("calculation finished")
      }
    }
  }

  private def readParamFile[T <: Param](file: String, fsType: String)(implicit m : Manifest[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file, fsType)
    paramReader.readConfig[T]
  }

  private def validateParams(allParam: AllParam): Try[Boolean] = {
    val allParamValidator = AllParamValidator()
    allParamValidator.validate(allParam)
  }

}
