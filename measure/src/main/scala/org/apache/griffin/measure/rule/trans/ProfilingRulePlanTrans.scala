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
package org.apache.griffin.measure.rule.trans

import org.apache.griffin.measure.process.temp.TableRegisters
import org.apache.griffin.measure.process.{BatchProcessType, ExportMode, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.adaptor.RuleParamKeys._
import org.apache.griffin.measure.rule.adaptor._
import org.apache.griffin.measure.rule.dsl.analyzer.ProfilingAnalyzer
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.rule.trans.RuleExportFactory._
import org.apache.griffin.measure.utils.ParamUtil._

import scala.util.Try

case class ProfilingRulePlanTrans(dataSourceNames: Seq[String],
                                  timeInfo: TimeInfo, name: String, expr: Expr,
                                  param: Map[String, Any], procType: ProcessType
                                 ) extends RulePlanTrans {

  private object ProfilingKeys {
    val _source = "source"
  }
  import ProfilingKeys._

  def trans(): Try[RulePlan] = Try {
    val details = getDetails(param)
    val profilingClause = expr.asInstanceOf[ProfilingClause]
    val sourceName = profilingClause.fromClauseOpt match {
      case Some(fc) => fc.dataSource
      case _ => details.getString(_source, dataSourceNames.head)
    }
    val fromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc

    val mode = ExportMode.defaultMode(procType)

    val ct = timeInfo.calcTime

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      emptyRulePlan
    } else {
      val analyzer = ProfilingAnalyzer(profilingClause, sourceName)
      val selExprDescs = analyzer.selectionExprs.map { sel =>
        val alias = sel match {
          case s: AliasableExpr if (s.alias.nonEmpty) => s" AS `${s.alias.get}`"
          case _ => ""
        }
        s"${sel.desc}${alias}"
      }
      val selCondition = profilingClause.selectClause.extraConditionOpt.map(_.desc).mkString
      val selClause = procType match {
        case BatchProcessType => selExprDescs.mkString(", ")
        case StreamingProcessType => (s"`${InternalColumns.tmst}`" +: selExprDescs).mkString(", ")
      }
      val groupByClauseOpt = analyzer.groupbyExprOpt
      val groupbyClause = procType match {
        case BatchProcessType => groupByClauseOpt.map(_.desc).getOrElse("")
        case StreamingProcessType => {
          val tmstGroupbyClause = GroupbyClause(LiteralStringExpr(s"`${InternalColumns.tmst}`") :: Nil, None)
          val mergedGroubbyClause = tmstGroupbyClause.merge(groupByClauseOpt match {
            case Some(gbc) => gbc
            case _ => GroupbyClause(Nil, None)
          })
          mergedGroubbyClause.desc
        }
      }
      val preGroupbyClause = analyzer.preGroupbyExprs.map(_.desc).mkString(" ")
      val postGroupbyClause = analyzer.postGroupbyExprs.map(_.desc).mkString(" ")

      // 1. select statement
      val profilingSql = {
        s"SELECT ${selCondition} ${selClause} ${fromClause} ${preGroupbyClause} ${groupbyClause} ${postGroupbyClause}"
      }
      val profilingName = name
      val profilingStep = SparkSqlStep(profilingName, profilingSql, details)
      val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
      val profilingExports = genMetricExport(metricParam, name, profilingName, ct, mode) :: Nil

      RulePlan(profilingStep :: Nil, profilingExports)
    }
  }

}
