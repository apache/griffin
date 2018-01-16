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

import java.util.concurrent.atomic.AtomicLong

import org.apache.griffin.measure.cache.tmst.TempName

import scala.collection.mutable.{Set => MutableSet}
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.{ExportMode, ProcessType}
import org.apache.griffin.measure.process.temp.TimeRange
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan.{TimeInfo, _}

//object RuleInfoKeys {
//  val _name = "name"
//  val _rule = "rule"
//  val _details = "details"
//  val _dslType = "dsl.type"
//  val _dqType = "dq.type"
//  val _global = "global"
////  val _gatherStep = "gather.step"
//
//  val _metric = "metric"
//  val _record = "record"
//}
//import RuleInfoKeys._
import org.apache.griffin.measure.utils.ParamUtil._

object RuleParamKeys {
  val _name = "name"
  val _rule = "rule"
  val _dslType = "dsl.type"
  val _dqType = "dq.type"
  val _cache = "cache"
  val _global = "global"
  val _details = "details"

  val _metric = "metric"
  val _record = "record"

  def getName(param: Map[String, Any], defName: String): String = param.getString(_name, defName)
  def getRule(param: Map[String, Any]): String = param.getString(_rule, "")
  def getDqType(param: Map[String, Any]): DqType = DqType(param.getString(_dqType, ""))
  def getCache(param: Map[String, Any]): Boolean = param.getBoolean(_cache, false)
  def getGlobal(param: Map[String, Any]): Boolean = param.getBoolean(_global, false)
  def getDetails(param: Map[String, Any]): Map[String, Any] = param.getParamMap(_details)

  def getMetricOpt(param: Map[String, Any]): Option[Map[String, Any]] = param.getParamMapOpt(_metric)
  def getRecordOpt(param: Map[String, Any]): Option[Map[String, Any]] = param.getParamMapOpt(_record)
}

object ExportParamKeys {
  val _name = "name"
  val _collectType = "collect.type"
  val _dataSourceCache = "data.source.cache"
  val _originDF = "origin.DF"

  def getName(param: Map[String, Any], defName: String): String = param.getString(_name, defName)
  def getCollectType(param: Map[String, Any]): CollectType = CollectType(param.getString(_collectType, ""))
  def getDataSourceCacheOpt(param: Map[String, Any]): Option[String] = param.get(_dataSourceCache).map(_.toString)
  def getOriginDFOpt(param: Map[String, Any]): Option[String] = param.get(_originDF).map(_.toString)
}

trait RuleAdaptor extends Loggable with Serializable {

//  val adaptPhase: AdaptPhase

//  protected def genRuleInfo(param: Map[String, Any]): RuleInfo = RuleInfoGen(param)

//  protected def getName(param: Map[String, Any]) = param.getOrElse(_name, RuleStepNameGenerator.genName).toString
//  protected def getRule(param: Map[String, Any]) = param.getOrElse(_rule, "").toString
//  protected def getDetails(param: Map[String, Any]) = param.get(_details) match {
//    case Some(dt: Map[String, Any]) => dt
//    case _ => Map[String, Any]()
//  }



//  def getPersistNames(steps: Seq[RuleStep]): Seq[String] = steps.map(_.ruleInfo.persistName)
//
//  protected def genRuleStep(timeInfo: TimeInfo, param: Map[String, Any]): Seq[RuleStep]
//  protected def adaptConcreteRuleStep(ruleStep: RuleStep): Seq[ConcreteRuleStep]
//  def genConcreteRuleStep(timeInfo: TimeInfo, param: Map[String, Any]
//                         ): Seq[ConcreteRuleStep] = {
//    genRuleStep(timeInfo, param).flatMap { rs =>
//      adaptConcreteRuleStep(rs)
//    }
//  }



//  def genRuleInfos(param: Map[String, Any], timeInfo: TimeInfo): Seq[RuleInfo] = {
//    RuleInfoGen(param) :: Nil
//  }

  protected def getRuleName(param: Map[String, Any]): String = {
    RuleParamKeys.getName(param, RuleStepNameGenerator.genName)
  }

  def genRulePlan(timeInfo: TimeInfo, param: Map[String, Any],
                  procType: ProcessType, dsTimeRanges: Map[String, TimeRange]): RulePlan

  protected def genRuleExports(param: Map[String, Any], defName: String,
                               stepName: String, defTimestamp: Long,
                               mode: ExportMode
                              ): Seq[RuleExport] = {
    val metricOpt = RuleParamKeys.getMetricOpt(param)
    val metricExportSeq = metricOpt.map(genMetricExport(_, defName, stepName, defTimestamp, mode)).toSeq
    val recordOpt = RuleParamKeys.getRecordOpt(param)
    val recordExportSeq = recordOpt.map(genRecordExport(_, defName, stepName, defTimestamp, mode)).toSeq
    metricExportSeq ++ recordExportSeq
  }
  protected def genMetricExport(param: Map[String, Any], name: String, stepName: String,
                                defTimestamp: Long, mode: ExportMode
                               ): MetricExport = {
    MetricExport(
      ExportParamKeys.getName(param, name),
      stepName,
      ExportParamKeys.getCollectType(param),
      defTimestamp,
      mode
    )
  }
  protected def genRecordExport(param: Map[String, Any], name: String, stepName: String,
                                defTimestamp: Long, mode: ExportMode
                               ): RecordExport = {
    RecordExport(
      ExportParamKeys.getName(param, name),
      stepName,
      ExportParamKeys.getDataSourceCacheOpt(param),
      ExportParamKeys.getOriginDFOpt(param),
      defTimestamp,
      mode
    )
  }



}



//object RuleInfoGen {
//  def apply(param: Map[String, Any]): RuleInfo = {
//    val name = param.get(_name) match {
//      case Some(n: String) => n
//      case _ => RuleStepNameGenerator.genName
//    }
//    RuleInfo(
//      name,
//      None,
//      DslType(param.getString(_dslType, "")),
//      param.getString(_rule, ""),
//      param.getParamMap(_details),
//      param.getBoolean(_gatherStep, false)
//    )
//  }
//  def apply(ri: RuleInfo, timeInfo: TimeInfo): RuleInfo = {
//    if (ri.persistType.needPersist) {
//      val tmstName = TempName.tmstName(ri.name, timeInfo)
//      ri.setTmstNameOpt(Some(tmstName))
//    } else ri
//  }
//
//  def dqType(param: Map[String, Any]): DqType = DqType(param.getString(_dqType, ""))
//}

object RuleStepNameGenerator {
  private val counter: AtomicLong = new AtomicLong(0L)
  private val head: String = "rs"

  def genName: String = {
    s"${head}${increment}"
  }

  private def increment: Long = {
    counter.incrementAndGet()
  }
}