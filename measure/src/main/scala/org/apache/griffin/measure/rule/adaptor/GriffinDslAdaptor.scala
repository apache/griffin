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

import org.apache.griffin.measure.cache.tmst.{TempName, TmstCache}
import org.apache.griffin.measure.process.engine.DataFrameOprs.AccuracyOprKeys
import org.apache.griffin.measure.process.temp.TableRegisters
import org.apache.griffin.measure.process._
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.dsl.analyzer._
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.rule.plan.{TimeInfo, _}
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.griffin.measure.utils.TimeUtil

object AccuracyKeys {
  val _source = "source"
  val _target = "target"
  val _miss = "miss"
  val _total = "total"
  val _matched = "matched"
//  val _missRecords = "missRecords"
}

object ProfilingKeys {
  val _source = "source"
}

object UniquenessKeys {
  val _source = "source"
  val _target = "target"
  val _unique = "unique"
  val _total = "total"
  val _dup = "dup"
  val _num = "num"
  val _duplicationArray = "duplication.array"
}

object TimelinessKeys {
  val _source = "source"
  val _latency = "latency"
  val _threshold = "threshold"
}

object GlobalKeys {
  val _initRule = "init.rule"
//  val _globalMetricKeep = "global.metric.keep"
}

case class GriffinDslAdaptor(dataSourceNames: Seq[String],
                             functionNames: Seq[String]
                            ) extends RuleAdaptor {

  import RuleParamKeys._

  val filteredFunctionNames = functionNames.filter { fn =>
    fn.matches("""^[a-zA-Z_]\w*$""")
  }
  val parser = GriffinDslParser(dataSourceNames, filteredFunctionNames)

  private val emptyRulePlan = RulePlan(Nil, Nil)
  private val emptyMap = Map[String, Any]()

  override def genRulePlan(timeInfo: TimeInfo, param: Map[String, Any], processType: ProcessType
                          ): RulePlan = {
    val name = getRuleName(param)
    val rule = getRule(param)
    val dqType = getDqType(param)
    try {
      val result = parser.parseRule(rule, dqType)
      if (result.successful) {
        val expr = result.get
        dqType match {
          case AccuracyType => accuracyRulePlan(timeInfo, name, expr, param, processType)
          case ProfilingType => profilingRulePlan(timeInfo, name, expr, param, processType)
          case UniquenessType => uniquenessRulePlan(timeInfo, name, expr, param, processType)
          case TimelinessType => timelinessRulePlan(timeInfo, name, expr, param, processType)
          case _ => emptyRulePlan
        }
      } else {
        warn(s"parse rule [ ${rule} ] fails: \n${result}")
        emptyRulePlan
      }
    } catch {
      case e: Throwable => {
        error(s"generate rule plan ${name} fails: ${e.getMessage}")
        emptyRulePlan
      }
    }
  }

  // with accuracy opr
  private def accuracyRulePlan(timeInfo: TimeInfo, name: String, expr: Expr,
                               param: Map[String, Any], processType: ProcessType
                              ): RulePlan = {
    val details = getDetails(param)
    val sourceName = details.getString(AccuracyKeys._source, dataSourceNames.head)
    val targetName = details.getString(AccuracyKeys._target, dataSourceNames.tail.head)
    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      println(s"[${timeInfo.calcTime}] data source ${sourceName} not exists")
      emptyRulePlan
    } else {
      // 1. miss record
      val missRecordsTableName = "__missRecords"
      val selClause = s"`${sourceName}`.*"
      val missRecordsSql = if (!TableRegisters.existRunTempTable(timeInfo.key, targetName)) {
        println(s"[${timeInfo.calcTime}] data source ${targetName} not exists")
        s"SELECT ${selClause} FROM `${sourceName}`"
      } else {
        val onClause = expr.coalesceDesc
        val sourceIsNull = analyzer.sourceSelectionExprs.map { sel =>
          s"${sel.desc} IS NULL"
        }.mkString(" AND ")
        val targetIsNull = analyzer.targetSelectionExprs.map { sel =>
          s"${sel.desc} IS NULL"
        }.mkString(" AND ")
        val whereClause = s"(NOT (${sourceIsNull})) AND (${targetIsNull})"
        s"SELECT ${selClause} FROM `${sourceName}` LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
      }
      val missRecordsStep = SparkSqlStep(missRecordsTableName, missRecordsSql, emptyMap, true)
      val missRecordsExports = processType match {
        case BatchProcessType => {
          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          genRecordExport(recordParam, missRecordsTableName, missRecordsTableName) :: Nil
        }
        case StreamingProcessType => Nil
      }

      // 2. miss count
      val missCountTableName = "__missCount"
      val missColName = details.getStringOrKey(AccuracyKeys._miss)
      val missCountSql = processType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}`"
        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}` GROUP BY `${InternalColumns.tmst}`"
      }
      val missCountStep = SparkSqlStep(missCountTableName, missCountSql, emptyMap)

      // 3. total count
      val totalCountTableName = "__totalCount"
      val totalColName = details.getStringOrKey(AccuracyKeys._total)
      val totalCountSql = processType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceName}` GROUP BY `${InternalColumns.tmst}`"
      }
      val totalCountStep = SparkSqlStep(totalCountTableName, totalCountSql, emptyMap)

      // 4. accuracy metric
      val accuracyTableName = name
      val matchedColName = details.getStringOrKey(AccuracyKeys._matched)
      val accuracyMetricSql = processType match {
        case BatchProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
             |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
         """.stripMargin
        }
        case StreamingProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
             |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
             |FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
             |ON `${totalCountTableName}`.`${InternalColumns.tmst}` = `${missCountTableName}`.`${InternalColumns.tmst}`
         """.stripMargin
        }
      }
      val accuracyStep = SparkSqlStep(accuracyTableName, accuracyMetricSql, emptyMap)
      val accuracyExports = processType match {
        case BatchProcessType => {
          val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
          genMetricExport(metricParam, accuracyTableName, accuracyTableName) :: Nil
        }
        case StreamingProcessType => Nil
      }

      // current accu plan
      val accuSteps = missRecordsStep :: missCountStep :: totalCountStep :: accuracyStep :: Nil
      val accuExports = missRecordsExports ++ accuracyExports
      val accuPlan = RulePlan(accuSteps, accuExports)

      // streaming extra accu plan
      val streamingAccuPlan = processType match {
        case BatchProcessType => emptyRulePlan
        case StreamingProcessType => {
          // 5. accuracy metric merge
          val accuracyMetricTableName = "__accuracy"
          val accuracyMetricRule = "accuracy"
          val accuracyMetricDetails = Map[String, Any](
            (AccuracyOprKeys._dfName -> accuracyTableName),
            (AccuracyOprKeys._miss -> missColName),
            (AccuracyOprKeys._total -> totalColName),
            (AccuracyOprKeys._matched -> matchedColName)
          )
          val accuracyMetricStep = DfOprStep(accuracyMetricTableName,
            accuracyMetricRule, accuracyMetricDetails)
          val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
          val accuracyMetricExports = genMetricExport(metricParam, name, accuracyMetricTableName) :: Nil

          // 6. collect accuracy records
          val accuracyRecordTableName = "__accuracyRecords"
          val accuracyRecordSql = {
            s"""
               |SELECT `${InternalColumns.tmst}`, `${InternalColumns.empty}`
               |FROM `${accuracyMetricTableName}` WHERE `${InternalColumns.record}`
             """.stripMargin
          }
          val accuracyRecordStep = SparkSqlStep(accuracyRecordTableName, accuracyRecordSql, emptyMap)
          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          val accuracyRecordParam = recordParam.addIfNotExist(ExportParamKeys._dataSourceCache, sourceName)
            .addIfNotExist(ExportParamKeys._originDF, missRecordsTableName)
          val accuracyRecordExports = genRecordExport(
            accuracyRecordParam, missRecordsTableName, accuracyRecordTableName) :: Nil

          // gen accu plan
          val extraSteps = accuracyMetricStep :: accuracyRecordStep :: Nil
          val extraExports = accuracyMetricExports ++ accuracyRecordExports
          val extraPlan = RulePlan(extraSteps, extraExports)

          extraPlan
        }
      }

      // return accu plan
      accuPlan.merge(streamingAccuPlan)

    }
  }

  private def profilingRulePlan(timeInfo: TimeInfo, name: String, expr: Expr,
                                param: Map[String, Any], processType: ProcessType
                               ): RulePlan = {
    val details = getDetails(param)
    val profilingClause = expr.asInstanceOf[ProfilingClause]
    val sourceName = profilingClause.fromClauseOpt match {
      case Some(fc) => fc.dataSource
      case _ => details.getString(ProfilingKeys._source, dataSourceNames.head)
    }
    val fromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc

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
      val selClause = processType match {
        case BatchProcessType => selExprDescs.mkString(", ")
        case StreamingProcessType => (s"`${InternalColumns.tmst}`" +: selExprDescs).mkString(", ")
      }
      val groupByClauseOpt = analyzer.groupbyExprOpt
      val groupbyClause = processType match {
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
      val profilingExports = genMetricExport(metricParam, name, profilingName) :: Nil

      RulePlan(profilingStep :: Nil, profilingExports)
    }
  }

  private def uniquenessRulePlan(timeInfo: TimeInfo, name: String, expr: Expr,
                                 param: Map[String, Any], processType: ProcessType
                                ): RulePlan = {
    val details = getDetails(param)
    val sourceName = details.getString(UniquenessKeys._source, dataSourceNames.head)
    val targetName = details.getString(UniquenessKeys._target, dataSourceNames.tail.head)
    val analyzer = UniquenessAnalyzer(expr.asInstanceOf[UniquenessClause], sourceName, targetName)

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      println(s"[${timeInfo.calcTime}] data source ${sourceName} not exists")
      emptyRulePlan
    } else if (!TableRegisters.existRunTempTable(timeInfo.key, targetName)) {
      println(s"[${timeInfo.calcTime}] data source ${targetName} not exists")
      emptyRulePlan
    } else {
      val selItemsClause = analyzer.selectionPairs.map { pair =>
        val (expr, alias) = pair
        s"${expr.desc} AS `${alias}`"
      }.mkString(", ")
      val aliases = analyzer.selectionPairs.map(_._2)

      val selClause = processType match {
        case BatchProcessType => selItemsClause
        case StreamingProcessType => s"`${InternalColumns.tmst}`, ${selItemsClause}"
      }
      val selAliases = processType match {
        case BatchProcessType => aliases
        case StreamingProcessType => InternalColumns.tmst +: aliases
      }

      // 1. source mapping
      val sourceTableName = "__source"
      val sourceSql = s"SELECT DISTINCT ${selClause} FROM ${sourceName}"
      val sourceStep = SparkSqlStep(sourceTableName, sourceSql, emptyMap)

      // 2. target mapping
      val targetTableName = "__target"
      val targetSql = s"SELECT ${selClause} FROM ${targetName}"
      val targetStep = SparkSqlStep(targetTableName, targetSql, emptyMap)

      // 3. joined
      val joinedTableName = "__joined"
      val joinedSelClause = selAliases.map { alias =>
        s"`${sourceTableName}`.`${alias}` AS `${alias}`"
      }.mkString(", ")
      val onClause = aliases.map { alias =>
        s"coalesce(`${sourceTableName}`.`${alias}`, '') = coalesce(`${targetTableName}`.`${alias}`, '')"
      }.mkString(" AND ")
      val joinedSql = {
        s"SELECT ${joinedSelClause} FROM `${targetTableName}` RIGHT JOIN `${sourceTableName}` ON ${onClause}"
      }
      val joinedStep = SparkSqlStep(joinedTableName, joinedSql, emptyMap)

      // 4. group
      val groupTableName = "__group"
      val groupSelClause = selAliases.map { alias =>
        s"`${alias}`"
      }.mkString(", ")
      val dupColName = details.getStringOrKey(UniquenessKeys._dup)
      val groupSql = {
        s"SELECT ${groupSelClause}, (COUNT(*) - 1) AS `${dupColName}` FROM `${joinedTableName}` GROUP BY ${groupSelClause}"
      }
      val groupStep = SparkSqlStep(groupTableName, groupSql, emptyMap, true)

      // 5. total metric
      val totalTableName = "__totalMetric"
      val totalColName = details.getStringOrKey(UniquenessKeys._total)
      val totalSql = processType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
        case StreamingProcessType => {
          s"""
             |SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}`
             |FROM `${sourceName}` GROUP BY `${InternalColumns.tmst}`
           """.stripMargin
        }
      }
      val totalStep = SparkSqlStep(totalTableName, totalSql, emptyMap)
      val totalMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
      val totalMetricExport = genMetricExport(totalMetricParam, totalColName, totalTableName)

      // 6. unique record
      val uniqueRecordTableName = "__uniqueRecord"
      val uniqueRecordSql = {
        s"SELECT * FROM `${groupTableName}` WHERE `${dupColName}` = 0"
      }
      val uniqueRecordStep = SparkSqlStep(uniqueRecordTableName, uniqueRecordSql, emptyMap)

      // 7. unique metric
      val uniqueTableName = "__uniqueMetric"
      val uniqueColName = details.getStringOrKey(UniquenessKeys._unique)
      val uniqueSql = processType match {
        case BatchProcessType => s"SELECT COUNT(*) AS `${uniqueColName}` FROM `${uniqueRecordTableName}`"
        case StreamingProcessType => {
          s"""
             |SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${uniqueColName}`
             |FROM `${uniqueRecordTableName}` GROUP BY `${InternalColumns.tmst}`
           """.stripMargin
        }
      }
      val uniqueStep = SparkSqlStep(uniqueTableName, uniqueSql, emptyMap)
      val uniqueMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
      val uniqueMetricExport = genMetricExport(uniqueMetricParam, uniqueColName, uniqueTableName)

      // 8. count metric
//      val countMetricTableName = "__countMetric"
//      val countMetricSql = processType match {
//        case BatchProcessType => {
//          s"""
//             |SELECT `${totalTableName}`.`${totalColName}` AS `${totalColName}`,
//             |coalesce(`${uniqueTableName}`.`${uniqueColName}`, 0) AS `${uniqueColName}`
//             |FROM `${totalTableName}` LEFT JOIN `${uniqueTableName}`
//          """.stripMargin
//        }
//        case StreamingProcessType => {
//          s"""
//             |SELECT `${totalTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
//             |`${totalTableName}`.`${totalColName}` AS `${totalColName}`,
//             |coalesce(`${uniqueTableName}`.`${uniqueColName}`, 0) AS `${uniqueColName}`
//             |FROM `${totalTableName}` LEFT JOIN `${uniqueTableName}`
//             |ON `${totalTableName}`.`${InternalColumns.tmst}` = `${uniqueTableName}`.`${InternalColumns.tmst}`
//          """.stripMargin
//        }
//      }
//      val countMetricStep = SparkSqlStep(countMetricTableName, countMetricSql, emptyMap)
//      val countMetricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
//        .addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
//      val countMetricExport = genMetricExport(countMetricParam, "", countMetricTableName)

      val uniqueSteps = sourceStep :: targetStep :: joinedStep :: groupStep ::
        totalStep :: uniqueRecordStep :: uniqueStep :: Nil
      val uniqueExports = totalMetricExport :: uniqueMetricExport :: Nil
      val uniqueRulePlan = RulePlan(uniqueSteps, uniqueExports)

      val duplicationArrayName = details.getString(UniquenessKeys._duplicationArray, "")
      val dupRulePlan = if (duplicationArrayName.nonEmpty) {
        // 8. duplicate record
        val dupRecordTableName = "__dupRecords"
        val dupRecordSql = {
          s"SELECT * FROM `${groupTableName}` WHERE `${dupColName}` > 0"
        }
        val dupRecordStep = SparkSqlStep(dupRecordTableName, dupRecordSql, emptyMap, true)
        val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
        val dupRecordExport = genRecordExport(recordParam, dupRecordTableName, dupRecordTableName)

        // 9. duplicate metric
        val dupMetricTableName = "__dupMetric"
        val numColName = details.getStringOrKey(UniquenessKeys._num)
        val dupMetricSelClause = processType match {
          case BatchProcessType => s"`${dupColName}`, COUNT(*) AS `${numColName}`"
          case StreamingProcessType => s"`${InternalColumns.tmst}`, `${dupColName}`, COUNT(*) AS `${numColName}`"
        }
        val dupMetricGroupbyClause = processType match {
          case BatchProcessType => s"`${dupColName}`"
          case StreamingProcessType => s"`${InternalColumns.tmst}`, `${dupColName}`"
        }
        val dupMetricSql = {
          s"""
             |SELECT ${dupMetricSelClause} FROM `${dupRecordTableName}`
             |GROUP BY ${dupMetricGroupbyClause}
          """.stripMargin
        }
        val dupMetricStep = SparkSqlStep(dupMetricTableName, dupMetricSql, emptyMap)
        val dupMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, ArrayCollectType.desc)
        val dupMetricExport = genMetricExport(dupMetricParam, duplicationArrayName, dupMetricTableName)

        RulePlan(dupRecordStep :: dupMetricStep :: Nil, dupRecordExport :: dupMetricExport :: Nil)
      } else emptyRulePlan

      uniqueRulePlan.merge(dupRulePlan)
    }
  }

  private def timelinessRulePlan(timeInfo: TimeInfo, name: String, expr: Expr,
                                 param: Map[String, Any], processType: ProcessType
                                ): RulePlan = {
    val details = getDetails(param)
    val timelinessClause = expr.asInstanceOf[TimelinessClause]
    val sourceName = details.getString(TimelinessKeys._source, dataSourceNames.head)

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
      val latencyColName = details.getStringOrKey(TimelinessKeys._latency)
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
      val metricSql = processType match {
        case BatchProcessType => {
          s"""
             |SELECT CAST(AVG(`${latencyColName}`) AS BIGINT) AS `avg`,
             |MAX(`${latencyColName}`) AS `max`,
             |MIN(`${latencyColName}`) AS `min`
             |FROM `${latencyTableName}`
           """.stripMargin
        }
        case StreamingProcessType => {
          s"""
             |SELECT `${InternalColumns.tmst}`,
             |CAST(AVG(`${latencyColName}`) AS BIGINT) AS `avg`,
             |MAX(`${latencyColName}`) AS `max`,
             |MIN(`${latencyColName}`) AS `min`
             |FROM `${latencyTableName}`
             |GROUP BY `${InternalColumns.tmst}`
           """.stripMargin
        }
      }
      val metricStep = SparkSqlStep(metricTableName, metricSql, emptyMap)
      val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
      val metricExports = genMetricExport(metricParam, name, metricTableName) :: Nil

      // current timeliness plan
      val timeSteps = inTimeStep :: latencyStep :: metricStep :: Nil
      val timeExports = metricExports
      val timePlan = RulePlan(timeSteps, timeExports)

      // 4. timeliness record
      val recordPlan = TimeUtil.milliseconds(details.getString(TimelinessKeys._threshold, "")) match {
        case Some(tsh) => {
          val recordTableName = "__lateRecords"
          val recordSql = {
            s"SELECT * FROM `${latencyTableName}` WHERE `${latencyColName}` > ${tsh}"
          }
          val recordStep = SparkSqlStep(recordTableName, recordSql, emptyMap)
          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          val recordExports = genRecordExport(recordParam, recordTableName, recordTableName) :: Nil
          RulePlan(recordStep :: Nil, recordExports)
        }
        case _ => emptyRulePlan
      }

      // return timeliness plan
      timePlan.merge(recordPlan)
    }
  }

}
