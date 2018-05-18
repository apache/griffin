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

import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.{ExportMode, ProcessType}
import org.apache.griffin.measure.process.temp.TimeRange
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan.{TimeInfo, _}
import org.apache.griffin.measure.rule.trans.{DsUpdateFactory, RuleExportFactory}
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
  val _dsUpdate = "ds.update"

  def getName(param: Map[String, Any], defName: String): String = param.getString(_name, defName)
  def getRule(param: Map[String, Any]): String = param.getString(_rule, "")
  def getDqType(param: Map[String, Any]): DqType = DqType(param.getString(_dqType, ""))
  def getCache(param: Map[String, Any]): Boolean = param.getBoolean(_cache, false)
  def getGlobal(param: Map[String, Any]): Boolean = param.getBoolean(_global, false)
  def getDetails(param: Map[String, Any]): Map[String, Any] = param.getParamMap(_details)

  def getMetricOpt(param: Map[String, Any]): Option[Map[String, Any]] = param.getParamMapOpt(_metric)
  def getRecordOpt(param: Map[String, Any]): Option[Map[String, Any]] = param.getParamMapOpt(_record)
  def getDsUpdateOpt(param: Map[String, Any]): Option[Map[String, Any]] = param.getParamMapOpt(_dsUpdate)
}

trait RuleAdaptor extends Loggable with Serializable {

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
    val metricExportSeq = metricOpt.map(
      RuleExportFactory.genMetricExport(_, defName, stepName, defTimestamp, mode)
    ).toSeq
    val recordOpt = RuleParamKeys.getRecordOpt(param)
    val recordExportSeq = recordOpt.map(
      RuleExportFactory.genRecordExport(_, defName, stepName, defTimestamp, mode)
    ).toSeq
    metricExportSeq ++ recordExportSeq
  }

  protected def genDsUpdates(param: Map[String, Any], defDsName: String,
                             stepName: String
                            ): Seq[DsUpdate] = {
    val dsUpdateOpt = RuleParamKeys.getDsUpdateOpt(param)
    dsUpdateOpt.map(DsUpdateFactory.genDsUpdate(_, defDsName, stepName)).toSeq
  }

}

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