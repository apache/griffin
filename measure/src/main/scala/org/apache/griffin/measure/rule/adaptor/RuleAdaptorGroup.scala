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

import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.process.ProcessType
import org.apache.griffin.measure.process.check.DataChecker
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.step._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.{Map => MutableMap}

object RuleAdaptorGroup {

  val _dslType = "dsl.type"

  var dataSourceNames: Seq[String] = _
  var functionNames: Seq[String] = _

  var dataChecker: DataChecker = _

  def init(sqlContext: SQLContext, dsNames: Seq[String]): Unit = {
    val functions = sqlContext.sql("show functions")
    functionNames = functions.map(_.getString(0)).collect
    dataSourceNames = dsNames

    dataChecker = DataChecker(sqlContext)
  }

  private def getDslType(param: Map[String, Any], defDslType: DslType) = {
    val dt = DslType(param.getOrElse(_dslType, "").toString)
    dt match {
      case UnknownDslType => defDslType
      case _ => dt
    }
  }

  private def genRuleAdaptor(dslType: DslType, dsNames: Seq[String], adaptPhase: AdaptPhase): Option[RuleAdaptor] = {
    dslType match {
      case SparkSqlType => Some(SparkSqlAdaptor(adaptPhase))
      case DfOprType => Some(DataFrameOprAdaptor(adaptPhase))
      case GriffinDslType => Some(GriffinDslAdaptor(dsNames, functionNames, adaptPhase))
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

  def genConcreteRuleSteps(evaluateRuleParam: EvaluateRuleParam,
                           adaptPhase: AdaptPhase
                          ): Seq[ConcreteRuleStep] = {
    val dslTypeStr = if (evaluateRuleParam.dslType == null) "" else evaluateRuleParam.dslType
    val defaultDslType = DslType(dslTypeStr)
    val ruleParams = evaluateRuleParam.rules
    genConcreteRuleSteps(ruleParams, defaultDslType, adaptPhase)
  }

  def genConcreteRuleSteps(ruleParams: Seq[Map[String, Any]],
                           defDslType: DslType, adaptPhase: AdaptPhase
                          ): Seq[ConcreteRuleStep] = {
    val (steps, dsNames) = ruleParams.foldLeft((Seq[ConcreteRuleStep](), dataSourceNames)) { (res, param) =>
      val (preSteps, preNames) = res
      val dslType = getDslType(param, defDslType)
      val (curSteps, curNames) = genRuleAdaptor(dslType, preNames, adaptPhase) match {
        case Some(ruleAdaptor) => (ruleAdaptor.genConcreteRuleStep(param), preNames ++ ruleAdaptor.getTempSourceNames(param))
        case _ => (Nil, preNames)
      }
      (preSteps ++ curSteps, curNames)
    }
    steps
  }


}
