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
import org.apache.griffin.measure.data.connector.InternalColumns
import org.apache.griffin.measure.process.ProcessType
import org.apache.griffin.measure.process.temp.TempTables
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.step._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.{Map => MutableMap}

object RuleAdaptorGroup {

//  val _dslType = "dsl.type"
  import RuleInfoKeys._

  var dataSourceNames: Seq[String] = Nil
  var functionNames: Seq[String] = Nil

  var baselineDsName: String = ""

  def init(dsNames: Seq[String], blDsName: String, funcNames: Seq[String]): Unit = {
    dataSourceNames = dsNames
    baselineDsName = blDsName
    functionNames = funcNames
  }

  def init(sqlContext: SQLContext, dsNames: Seq[String], blDsName: String): Unit = {
    val functions = sqlContext.sql("show functions")
    functionNames = functions.map(_.getString(0)).collect.toSeq
    dataSourceNames = dsNames

    baselineDsName = blDsName
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
    val tmsts = dsTmsts.getOrElse(baselineDsName, Set[Long]()).toSeq
    genRuleSteps(timeInfo, ruleParams, tmsts, defaultDslType)
  }

  def genRuleSteps(timeInfo: TimeInfo, ruleParams: Seq[Map[String, Any]],
                   tmsts: Seq[Long], defaultDslType: DslType,
                   adapthase: AdaptPhase = RunPhase
                  ): Seq[ConcreteRuleStep] = {
    val calcTime = timeInfo.calcTime
    val (ruleInfos, dsNames) = ruleParams.foldLeft((Seq[RuleInfo](), dataSourceNames)) { (res, param) =>
      val (preRuleInfos, preNames) = res
      val dslType = getDslType(param, defaultDslType)
      val (curRuleInfos, curNames) = genRuleAdaptor(dslType, preNames) match {
        case Some(adaptor) => {
          val ris = adaptor.genRuleInfos(param, timeInfo)
          val rins = ris.filter(!_.global).map(_.name)
          (ris, rins)
        }
        case _ => (Nil, Nil)
      }
      if (adapthase == RunPhase) {
        curNames.foreach(TempTables.registerTempTableNameOnly(timeInfo.key, _))
      }
      (preRuleInfos ++ curRuleInfos, preNames ++ curNames)
    }

    adapthase match {
      case PreProcPhase => {
        ruleInfos.flatMap { ri =>
          genConcRuleSteps(timeInfo, ri)
        }
      }
      case RunPhase => {
        val riGroups = ruleInfos.foldRight(List[(List[RuleInfo], Boolean)]()) { (ri, groups) =>
          groups match {
            case head :: tail if (ri.gather == head._2) => (ri :: head._1, head._2) :: tail
            case _ => (ri :: Nil, ri.gather) :: groups
          }
        }.foldLeft(List[(List[RuleInfo], Boolean, List[String], List[RuleInfo])]()) { (groups, rigs) =>
          val preGatherNames = groups.lastOption match {
            case Some(t) => if (t._2) t._3 ::: t._1.map(_.name) else t._3
            case _ => baselineDsName :: Nil
          }
          val persistRuleInfos = groups.lastOption match {
            case Some(t) if (t._2) => t._1.filter(_.persistType.needPersist)
            case _ => Nil
          }
          groups :+ (rigs._1, rigs._2, preGatherNames, persistRuleInfos)
        }

        riGroups.flatMap { group =>
          val (ris, gather, srcNames, persistRis) = group
          if (gather) {
            ris.flatMap { ri =>
              genConcRuleSteps(timeInfo, ri)
            }
          } else {
            tmsts.flatMap { tmst =>
              val concTimeInfo = TmstTimeInfo(calcTime, tmst)
              val tmstInitRuleInfos = genTmstInitRuleInfo(concTimeInfo, srcNames, persistRis)
              (tmstInitRuleInfos ++ ris).flatMap { ri =>
                genConcRuleSteps(concTimeInfo, ri)
              }
            }
          }
        }
      }
    }


  }

  private def genConcRuleSteps(timeInfo: TimeInfo, ruleInfo: RuleInfo): Seq[ConcreteRuleStep] = {
    val nri = if (ruleInfo.persistType.needPersist && ruleInfo.tmstNameOpt.isEmpty) {
      val tmstName = if (ruleInfo.gather) {
        TempName.tmstName(ruleInfo.name, timeInfo.calcTime)
      } else {
        TempName.tmstName(ruleInfo.name, timeInfo)
      }
      ruleInfo.setTmstNameOpt(Some(tmstName))
    } else ruleInfo
    ruleInfo.dslType match {
      case SparkSqlType => SparkSqlStep(timeInfo, nri) :: Nil
      case DfOprType => DfOprStep(timeInfo, nri) :: Nil
      case _ => Nil
    }
  }

  private def genTmstInitRuleInfo(timeInfo: TmstTimeInfo, srcNames: Seq[String],
                                  persistRis: Seq[RuleInfo]): Seq[RuleInfo] = {
    val TmstTimeInfo(calcTime, tmst, _) = timeInfo
    srcNames.map { srcName =>
      val srcTmstName = TempName.tmstName(srcName, calcTime)
      val filterSql = {
        s"SELECT * FROM `${srcTmstName}` WHERE `${InternalColumns.tmst}` = ${tmst}"
      }
      val params = persistRis.filter(_.name == srcName).headOption match {
        case Some(ri) => ri.details
        case _ => Map[String, Any]()
      }
      RuleInfo(srcName, None, SparkSqlType, filterSql, params, false)
    }
  }

//  def genRuleSteps(timeInfo: TimeInfo, ruleParams: Seq[Map[String, Any]],
//                   tmsts: Seq[Long], defaultDslType: DslType,
//                   adapthase: AdaptPhase = RunPhase
//                  ): Seq[ConcreteRuleStep] = {
//    tmsts.flatMap { tmst =>
//      val newTimeInfo = TimeInfo(timeInfo.calcTime, tmst)
//      val initSteps: Seq[ConcreteRuleStep] = adapthase match {
//        case RunPhase => genTmstInitStep(newTimeInfo)
//        case PreProcPhase => Nil
//      }
//      val (steps, dsNames) = ruleParams.foldLeft((initSteps, dataSourceNames)) { (res, param) =>
//        val (preSteps, preNames) = res
//        val dslType = getDslType(param, defaultDslType)
//        val (curSteps, curNames) = genRuleAdaptor(dslType, preNames) match {
//          case Some(ruleAdaptor) => {
//            val concreteSteps = ruleAdaptor.genConcreteRuleStep(newTimeInfo, param)
//            val persistNames = ruleAdaptor.getPersistNames(concreteSteps)
//            (concreteSteps, persistNames)
//          }
//          case _ => (Nil, Nil)
//        }
//        (preSteps ++ curSteps, preNames ++ curNames)
//      }
//      steps
//    }
//  }



//  private def genTmstInitStep(timeInfo: TimeInfo): Seq[ConcreteRuleStep] = {
//    val TimeInfo(calcTime, tmst) = timeInfo
//    val tmstDsName = TempName.tmstName(baselineDsName, calcTime)
//    val filterSql = {
//      s"SELECT * FROM `${tmstDsName}` WHERE `${InternalColumns.tmst}` = ${tmst}"
//    }
//    SparkSqlStep(
//      timeInfo,
//      RuleInfo(baselineDsName, None, filterSql, Map[String, Any]())
//    ) :: Nil
//  }


}
