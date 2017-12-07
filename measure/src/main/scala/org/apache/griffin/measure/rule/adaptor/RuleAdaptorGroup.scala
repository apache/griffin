/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.rule.adaptor

import org.apache.griffin.measure.cache.tmst.TempName
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.process.ProcessType
import org.apache.griffin.measure.process.check.DataChecker
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.step._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.{Map => MutableMap}

object RuleAdaptorGroup {

//  val _dslType = "dsl.type"
  import RuleInfoKeys._

  var dataSourceNames: Seq[String] = _
  var functionNames: Seq[String] = _

  var baselineDsName: String = ""

  var dataChecker: DataChecker = _

  def init(sqlContext: SQLContext, dsNames: Seq[String], blDsName: String): Unit = {
    val functions = sqlContext.sql("show functions")
    functionNames = functions.map(_.getString(0)).collect
    dataSourceNames = dsNames

    baselineDsName = blDsName

    dataChecker = DataChecker(sqlContext)
  }

  private def getDslType(param: Map[String, Any], defDslType: DslType) = {
    DslType(param.getOrElse(_dslType, defDslType.desc).toString)
  }

  private def genRuleAdaptor(dslType: DslType, dsNames: Seq[String]
                            ): Option[RuleAdaptor] = {
    dslType match {
      case SparkSqlType => Some(SparkSqlAdaptor())
      case DfOprType => Some(DataFrameOprAdaptor())
      case GriffinDslType => Some(GriffinDslAdaptor(dsNames, functionNames))
      case _ => None
    }
  }

//  def genRuleSteps(evaluateRuleParam: EvaluateRuleParam): Seq[RuleStep] = {
//    val dslTypeStr = if (evaluateRuleParam.dslType == null) "" else evaluateRuleParam.dslType
//    val defaultDslType = DslType(dslTypeStr)
//    val rules = evaluateRuleParam.rules
//    var dsNames = dataSourceNames
//    val steps = rules.flatMap { param =>
//      val dslType = getDslType(param)
//      genRuleAdaptor(dslType) match {
//        case Some(ruleAdaptor) => ruleAdaptor.genRuleStep(param)
//        case _ => Nil
//      }
//    }
//    steps.foreach(println)
//    steps
//  }

//  def genConcreteRuleSteps(timeInfo: TimeInfo, evaluateRuleParam: EvaluateRuleParam,
//                           dsTmsts: Map[String, Set[Long]], procType: ProcessType,
//                           adaptPhase: AdaptPhase
//                          ): Seq[ConcreteRuleStep] = {
//    val dslTypeStr = if (evaluateRuleParam.dslType == null) "" else evaluateRuleParam.dslType
//    val defaultDslType = DslType(dslTypeStr)
//    val ruleParams = evaluateRuleParam.rules
//    genConcreteRuleSteps(timeInfo, ruleParams, dsTmsts, defaultDslType, procType, adaptPhase)
//  }
//
//  def genConcreteRuleSteps(timeInfo: TimeInfo, ruleParams: Seq[Map[String, Any]],
//                           dsTmsts: Map[String, Set[Long]], defDslType: DslType,
//                           procType: ProcessType, adaptPhase: AdaptPhase
//                          ): Seq[ConcreteRuleStep] = {
//    val (steps, dsNames) = ruleParams.foldLeft((Seq[ConcreteRuleStep](), dataSourceNames)) { (res, param) =>
//      val (preSteps, preNames) = res
//      val dslType = getDslType(param, defDslType)
//      val (curSteps, curNames) = genRuleAdaptor(dslType, preNames, procType, adaptPhase) match {
//        case Some(ruleAdaptor) => {
//          val concreteSteps = ruleAdaptor.genConcreteRuleStep(timeInfo, param, dsTmsts)
//          (concreteSteps, preNames ++ ruleAdaptor.getPersistNames(concreteSteps))
//        }
//        case _ => (Nil, preNames)
//      }
//      (preSteps ++ curSteps, curNames)
//    }
//    steps
//  }


  // -- gen steps --
  def genRuleSteps(timeInfo: TimeInfo, evaluateRuleParam: EvaluateRuleParam, dsTmsts: Map[String, Set[Long]]
                  ): Seq[ConcreteRuleStep] = {
    val dslTypeStr = if (evaluateRuleParam.dslType == null) "" else evaluateRuleParam.dslType
    val defaultDslType = DslType(dslTypeStr)
    val ruleParams = evaluateRuleParam.rules
    genRuleSteps(timeInfo, ruleParams, dsTmsts, defaultDslType)
  }

  def genRuleSteps(timeInfo: TimeInfo, ruleParams: Seq[Map[String, Any]],
                   dsTmsts: Map[String, Set[Long]], defaultDslType: DslType,
                   adapthase: AdaptPhase = RunPhase
                  ): Seq[ConcreteRuleStep] = {
    val tmsts = dsTmsts.getOrElse(baselineDsName, Set[Long]()).toSeq
    tmsts.flatMap { tmst =>
      val newTimeInfo = TimeInfo(timeInfo.calcTime, tmst)
      val initSteps = adapthase match {
        case RunPhase => genTmstInitStep(newTimeInfo)
        case PreProcPhase => Nil
      }
      val (steps, dsNames) = ruleParams.foldLeft((initSteps, dataSourceNames)) { (res, param) =>
        val (preSteps, preNames) = res
        val dslType = getDslType(param, defaultDslType)
        val (curSteps, curNames) = genRuleAdaptor(dslType, preNames) match {
          case Some(ruleAdaptor) => {
            val concreteSteps = ruleAdaptor.genConcreteRuleStep(newTimeInfo, param)
            (concreteSteps, preNames ++ ruleAdaptor.getPersistNames(concreteSteps))
          }
          case _ => (Nil, preNames)
        }
        (preSteps ++ curSteps, curNames)
      }
//      steps.foreach(println)
      steps
    }
  }

  private def genTmstInitStep(timeInfo: TimeInfo): Seq[ConcreteRuleStep] = {
    val TimeInfo(calcTime, tmst) = timeInfo
    val tmstDsName = TempName.tmstName(baselineDsName, calcTime)
    val filterSql = {
      s"SELECT * FROM `${tmstDsName}` WHERE `${GroupByColumn.tmst}` = ${tmst}"
    }
    SparkSqlStep(
      timeInfo,
      RuleInfo(baselineDsName, None, filterSql, Map[String, Any]())
    ) :: Nil
  }


}
