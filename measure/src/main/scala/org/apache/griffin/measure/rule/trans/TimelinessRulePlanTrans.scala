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

import org.apache.griffin.measure.process.temp.{TableRegisters, TimeRange}
import org.apache.griffin.measure.process.{BatchProcessType, ExportMode, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.adaptor.RuleParamKeys._
import org.apache.griffin.measure.rule.adaptor._
import org.apache.griffin.measure.rule.dsl.ArrayCollectType
import org.apache.griffin.measure.rule.dsl.analyzer.TimelinessAnalyzer
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.rule.trans.RuleExportFactory._
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.griffin.measure.utils.TimeUtil

import scala.util.Try

case class TimelinessRulePlanTrans(dataSourceNames: Seq[String],
                                   timeInfo: TimeInfo, name: String, expr: Expr,
                                   param: Map[String, Any], procType: ProcessType,
                                   dsTimeRanges: Map[String, TimeRange]
                                  ) extends RulePlanTrans {

  private object TimelinessKeys {
    val _source = "source"
    val _latency = "latency"
    val _total = "total"
    val _avg = "avg"
    val _threshold = "threshold"
    val _step = "step"
    val _count = "count"
    val _stepSize = "step.size"
    val _percentileColPrefix = "percentile"
    val _percentileValues = "percentile.values"
  }
  import TimelinessKeys._

  def trans(): Try[RulePlan] =  Try {
    val details = getDetails(param)
    val timelinessClause = expr.asInstanceOf[TimelinessClause]
    val sourceName = details.getString(_source, dataSourceNames.head)

    val mode = ExportMode.defaultMode(procType)

//    val ct = timeInfo.calcTime

    val minTmstOpt = dsTimeRanges.get(sourceName).flatMap(_.minTmstOpt)
    val minTmst = minTmstOpt match {
      case Some(t) => t
      case _ => throw new Exception(s"empty min tmst from ${sourceName}")
    }

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      emptyRulePlan
    } else {
      val analyzer = TimelinessAnalyzer(timelinessClause, sourceName)
      val btsSel = analyzer.btsExpr
      val etsSelOpt = analyzer.etsExprOpt

      // 1. in time
      val inTimeTableName = "__inTime"
      val inTimeSql = etsSelOpt match {
        case Some(etsSel) => {
          s"""
             |SELECT *, (${btsSel}) AS `${InternalColumns.beginTs}`,
             |(${etsSel}) AS `${InternalColumns.endTs}`
             |FROM ${sourceName} WHERE (${btsSel}) IS NOT NULL AND (${etsSel}) IS NOT NULL
           """.stripMargin
        }
        case _ => {
          s"""
             |SELECT *, (${btsSel}) AS `${InternalColumns.beginTs}`
             |FROM ${sourceName} WHERE (${btsSel}) IS NOT NULL
           """.stripMargin
        }
      }
      val inTimeStep = SparkSqlStep(inTimeTableName, inTimeSql, emptyMap)

      // 2. latency
      val latencyTableName = "__lat"
      val latencyColName = details.getStringOrKey(_latency)
      val etsColName = etsSelOpt match {
        case Some(_) => InternalColumns.endTs
        case _ => InternalColumns.tmst
      }
      val latencySql = {
        s"SELECT *, (`${etsColName}` - `${InternalColumns.beginTs}`) AS `${latencyColName}` FROM `${inTimeTableName}`"
      }
      val latencyStep = SparkSqlStep(latencyTableName, latencySql, emptyMap, true)

      // 3. timeliness metric
      val metricTableName = name
      val totalColName = details.getStringOrKey(_total)
      val avgColName = details.getStringOrKey(_avg)
      val metricSql = procType match {
        case BatchProcessType => {
          s"""
             |SELECT COUNT(*) AS `${totalColName}`,
             |CAST(AVG(`${latencyColName}`) AS BIGINT) AS `${avgColName}`
             |FROM `${latencyTableName}`
           """.stripMargin
        }
        case StreamingProcessType => {
          s"""
             |SELECT `${InternalColumns.tmst}`,
             |COUNT(*) AS `${totalColName}`,
             |CAST(AVG(`${latencyColName}`) AS BIGINT) AS `${avgColName}`
             |FROM `${latencyTableName}`
             |GROUP BY `${InternalColumns.tmst}`
           """.stripMargin
        }
      }
      val metricStep = SparkSqlStep(metricTableName, metricSql, emptyMap)
      val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
      val metricExports = genMetricExport(metricParam, name, metricTableName, minTmst, mode) :: Nil

      // current timeliness plan
      val timeSteps = inTimeStep :: latencyStep :: metricStep :: Nil
      val timeExports = metricExports
      val timePlan = RulePlan(timeSteps, timeExports)

      // 4. timeliness record
      val recordPlan = TimeUtil.milliseconds(details.getString(_threshold, "")) match {
        case Some(tsh) => {
          val recordTableName = "__lateRecords"
          val recordSql = {
            s"SELECT * FROM `${latencyTableName}` WHERE `${latencyColName}` > ${tsh}"
          }
          val recordStep = SparkSqlStep(recordTableName, recordSql, emptyMap)
          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          val recordExports = genRecordExport(recordParam, recordTableName, recordTableName, minTmst, mode) :: Nil
          RulePlan(recordStep :: Nil, recordExports)
        }
        case _ => emptyRulePlan
      }

      // 5. ranges
      val rangePlan = TimeUtil.milliseconds(details.getString(_stepSize, "")) match {
        case Some(stepSize) => {
          // 5.1 range
          val rangeTableName = "__range"
          val stepColName = details.getStringOrKey(_step)
          val rangeSql = {
            s"""
               |SELECT *, CAST((`${latencyColName}` / ${stepSize}) AS BIGINT) AS `${stepColName}`
               |FROM `${latencyTableName}`
             """.stripMargin
          }
          val rangeStep = SparkSqlStep(rangeTableName, rangeSql, emptyMap)

          // 5.2 range metric
          val rangeMetricTableName = "__rangeMetric"
          val countColName = details.getStringOrKey(_count)
          val rangeMetricSql = procType match {
            case BatchProcessType => {
              s"""
                 |SELECT `${stepColName}`, COUNT(*) AS `${countColName}`
                 |FROM `${rangeTableName}` GROUP BY `${stepColName}`
                """.stripMargin
            }
            case StreamingProcessType => {
              s"""
                 |SELECT `${InternalColumns.tmst}`, `${stepColName}`, COUNT(*) AS `${countColName}`
                 |FROM `${rangeTableName}` GROUP BY `${InternalColumns.tmst}`, `${stepColName}`
                """.stripMargin
            }
          }
          val rangeMetricStep = SparkSqlStep(rangeMetricTableName, rangeMetricSql, emptyMap)
          val rangeMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, ArrayCollectType.desc)
          val rangeMetricExports = genMetricExport(rangeMetricParam, stepColName, rangeMetricTableName, minTmst, mode) :: Nil

          RulePlan(rangeStep :: rangeMetricStep :: Nil, rangeMetricExports)
        }
        case _ => emptyRulePlan
      }

      // 6. percentiles
      val percentiles = getPercentiles(details)
      val percentilePlan = if (percentiles.size > 0) {
        val percentileTableName = "__percentile"
        val percentileColName = details.getStringOrKey(_percentileColPrefix)
        val percentileCols = percentiles.map { pct =>
          val pctName = (pct * 100).toInt.toString
          s"floor(percentile_approx(${latencyColName}, ${pct})) AS `${percentileColName}_${pctName}`"
        }.mkString(", ")
        val percentileSql = {
          s"""
             |SELECT ${percentileCols}
             |FROM `${latencyTableName}`
            """.stripMargin
        }
        val percentileStep = SparkSqlStep(percentileTableName, percentileSql, emptyMap)
        val percentileParam = emptyMap
        val percentileExports = genMetricExport(percentileParam, percentileTableName, percentileTableName, minTmst, mode) :: Nil

        RulePlan(percentileStep :: Nil, percentileExports)
      } else emptyRulePlan

      timePlan.merge(recordPlan).merge(rangePlan).merge(percentilePlan)
    }
  }

  private def getPercentiles(details: Map[String, Any]): Seq[Double] = {
    details.getArr[Double](_percentileValues).filter(d => (d >= 0 && d <= 1))
  }

}
