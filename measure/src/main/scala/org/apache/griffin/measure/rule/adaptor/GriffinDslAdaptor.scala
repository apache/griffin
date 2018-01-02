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
          case TimelinessType => emptyRulePlan
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
             |FROM `${totalCountTableName}` FULL JOIN `${missCountTableName}`
         """.stripMargin
        }
        case StreamingProcessType => {
          s"""
             |SELECT `${totalCountTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
             |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
             |FROM `${totalCountTableName}` FULL JOIN `${missCountTableName}`
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

//  private def accuracyRulePlan(timeInfo: TimeInfo, name: String, expr: Expr,
//                               param: Map[String, Any], processType: ProcessType
//                              ): RulePlan = {
//    val details = getDetails(param)
//    val sourceName = details.getString(AccuracyKeys._source, dataSourceNames.head)
//    val targetName = details.getString(AccuracyKeys._target, dataSourceNames.tail.head)
//    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)
//
//    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
//      emptyRulePlan
//    } else {
//      // 1. miss record
//      val missRecordsTableName = "__missRecords"
//      val selClause = s"`${sourceName}`.*"
//      val missRecordsSql = if (!TableRegisters.existRunTempTable(timeInfo.key, targetName)) {
//        s"SELECT ${selClause} FROM `${sourceName}`"
//      } else {
//        val onClause = expr.coalesceDesc
//        val sourceIsNull = analyzer.sourceSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val targetIsNull = analyzer.targetSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val whereClause = s"(NOT (${sourceIsNull})) AND (${targetIsNull})"
//        s"SELECT ${selClause} FROM `${sourceName}` LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
//      }
//      val missRecordsStep = SparkSqlStep(missRecordsTableName, missRecordsSql, emptyMap, true)
//      val missRecordsExports = processType match {
//        case BatchProcessType => {
//          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
//          genRecordExport(recordParam, missRecordsTableName, missRecordsTableName) :: Nil
//        }
//        case StreamingProcessType => Nil
//      }
//
//      // 2. miss count
//      val missCountTableName = "__missCount"
//      val missColName = details.getStringOrKey(AccuracyKeys._miss)
//      val missCountSql = processType match {
//        case BatchProcessType => s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}`"
//        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}` GROUP BY `${InternalColumns.tmst}`"
//      }
//      val missCountStep = SparkSqlStep(missCountTableName, missCountSql, emptyMap)
//
//      // 3. total count
//      val totalCountTableName = "__totalCount"
//      val totalColName = details.getStringOrKey(AccuracyKeys._total)
//      val totalCountSql = processType match {
//        case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
//        case StreamingProcessType => s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceName}` GROUP BY `${InternalColumns.tmst}`"
//      }
//      val totalCountStep = SparkSqlStep(totalCountTableName, totalCountSql, emptyMap)
//
//      // 4. accuracy metric
//      val accuracyTableName = name
//      val matchedColName = details.getStringOrKey(AccuracyKeys._matched)
//      val accuracyMetricSql = processType match {
//        case BatchProcessType => {
//          s"""
//             |SELECT `${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
//             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
//             |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//             |FROM `${totalCountTableName}` FULL JOIN `${missCountTableName}`
//         """.stripMargin
//        }
//        case StreamingProcessType => {
//          s"""
//             |SELECT `${totalCountTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
//             |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
//             |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
//             |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//             |FROM `${totalCountTableName}` FULL JOIN `${missCountTableName}`
//             |ON `${totalCountTableName}`.`${InternalColumns.tmst}` = `${missCountTableName}`.`${InternalColumns.tmst}`
//         """.stripMargin
//        }
//      }
//      val accuracyStep = SparkSqlStep(accuracyTableName, accuracyMetricSql, emptyMap, true)
//      val accuracyExports = processType match {
//        case BatchProcessType => {
//          val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
//          genMetricExport(metricParam, accuracyTableName, accuracyTableName) :: Nil
//        }
//        case StreamingProcessType => Nil
//      }
//
//      // current accu plan
//      val accuSteps = missRecordsStep :: missCountStep :: totalCountStep :: accuracyStep :: Nil
//      val accuExports = missRecordsExports ++ accuracyExports
//      val accuPlan = RulePlan(accuSteps, accuExports)
//
//      // streaming extra accu plan
//      val streamingAccuPlan = processType match {
//        case BatchProcessType => emptyRulePlan
//        case StreamingProcessType => {
//          // 5. global accuracy metric merge
//          val globalAccuracyTableName = "__globalAccuracy"
//          val globalAccuracySql = {
//            s"""
//               |SELECT coalesce(`${globalAccuracyTableName}`.`${InternalColumns.tmst}`, `${accuracyTableName}`.`${InternalColumns.tmst}`) AS `${InternalColumns.tmst}`,
//               |coalesce(`${accuracyTableName}`.`${missColName}`, `${globalAccuracyTableName}`.`${missColName}`) AS `${missColName}`,
//               |coalesce(`${globalAccuracyTableName}`.`${totalColName}`, `${accuracyTableName}`.`${totalColName}`) AS `${totalColName}`,
//               |((`${accuracyTableName}`.`${missColName}` IS NOT NULL) AND ((`${globalAccuracyTableName}`.`${missColName}` IS NULL) OR (`${accuracyTableName}`.`${missColName}` < `${globalAccuracyTableName}`.`${missColName}`))) AS `${InternalColumns.metric}`
//               |FROM `${globalAccuracyTableName}` FULL JOIN `${accuracyTableName}`
//               |ON `${globalAccuracyTableName}`.`${InternalColumns.tmst}` = `${accuracyTableName}`.`${InternalColumns.tmst}`
//            """.stripMargin
//          }
//          val globalAccuracyInitSql = {
//            s"""
//               |SELECT `${InternalColumns.tmst}`, `${totalColName}`, `${missColName}`,
//               |(true) AS `${InternalColumns.metric}`
//               |FROM `${accuracyTableName}`
//             """.stripMargin
//          }
//          val globalAccuracyDetails = Map[String, Any](GlobalKeys._initRule -> globalAccuracyInitSql)
//          val globalAccuracyStep = SparkSqlStep(globalAccuracyTableName,
//            globalAccuracySql, globalAccuracyDetails, true, true)
//
//          // 6. collect accuracy metrics
//          val accuracyMetricTableName = name
//          val accuracyMetricSql = {
//            s"""
//               |SELECT `${InternalColumns.tmst}`, `${totalColName}`, `${missColName}`,
//               |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//               |FROM `${globalAccuracyTableName}` WHERE `${InternalColumns.metric}`
//             """.stripMargin
//          }
//          val accuracyMetricStep = SparkSqlStep(accuracyMetricTableName, accuracyMetricSql, emptyMap)
//          val metricParam = RuleParamKeys.getMetricOpt(param).getOrElse(emptyMap)
//          val accuracyMetricExports = genMetricExport(metricParam, accuracyMetricTableName, accuracyMetricTableName) :: Nil
//
//          // 7. collect accuracy records
//          val accuracyRecordTableName = "__accuracyRecords"
//          val accuracyRecordSql = {
//            s"""
//               |SELECT `${InternalColumns.tmst}`
//               |FROM `${accuracyMetricTableName}` WHERE `${matchedColName}` > 0
//             """.stripMargin
//          }
//          val accuracyRecordStep = SparkSqlStep(accuracyRecordTableName, accuracyRecordSql, emptyMap)
//          val recordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
//          val accuracyRecordParam = recordParam.addIfNotExist(ExportParamKeys._dataSourceCache, sourceName)
//            .addIfNotExist(ExportParamKeys._originDF, missRecordsTableName)
//          val accuracyRecordExports = genRecordExport(
//            accuracyRecordParam, missRecordsTableName, accuracyRecordTableName) :: Nil
//
//          // 8. update global accuracy metric
//          val updateGlobalAccuracyTableName = globalAccuracyTableName
//          val globalMetricKeepTime = details.getString(GlobalKeys._globalMetricKeep, "")
//          val updateGlobalAccuracySql = TimeUtil.milliseconds(globalMetricKeepTime) match {
//            case Some(kt) => {
//              s"""
//                 |SELECT * FROM `${globalAccuracyTableName}`
//                 |WHERE (`${missColName}` > 0) AND (`${InternalColumns.tmst}` > ${timeInfo.calcTime - kt})
//               """.stripMargin
//            }
//            case _ => {
//              s"""
//                 |SELECT * FROM `${globalAccuracyTableName}`
//                 |WHERE (`${missColName}` > 0)
//               """.stripMargin
//            }
//          }
//          val updateGlobalAccuracyStep = SparkSqlStep(updateGlobalAccuracyTableName,
//            updateGlobalAccuracySql, emptyMap, true, true)
//
//          // gen accu plan
//          val extraSteps = globalAccuracyStep :: accuracyMetricStep :: accuracyRecordStep :: updateGlobalAccuracyStep :: Nil
//          val extraExports = accuracyMetricExports ++ accuracyRecordExports
//          val extraPlan = RulePlan(extraSteps, extraExports)
//
//          extraPlan
//        }
//      }
//
//      // return accu plan
//      accuPlan.merge(streamingAccuPlan)
//
//    }
//  }

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
      val profilingExports = genMetricExport(metricParam, profilingName, profilingName) :: Nil

      RulePlan(profilingStep :: Nil, profilingExports)
    }
  }

//  override def genRuleInfos(param: Map[String, Any], timeInfo: TimeInfo): Seq[RuleInfo] = {
//    val ruleInfo = RuleInfoGen(param)
//    val dqType = RuleInfoGen.dqType(param)
//    try {
//      val result = parser.parseRule(ruleInfo.rule, dqType)
//      if (result.successful) {
//        val expr = result.get
//        dqType match {
//          case AccuracyType => accuracyRuleInfos(ruleInfo, expr, timeInfo)
//          case ProfilingType => profilingRuleInfos(ruleInfo, expr, timeInfo)
//          case TimelinessType => Nil
//          case _ => Nil
//        }
//      } else {
//        warn(s"parse rule [ ${ruleInfo.rule} ] fails: \n${result}")
//        Nil
//      }
//    } catch {
//      case e: Throwable => {
//        error(s"generate rule info ${ruleInfo} fails: ${e.getMessage}")
//        Nil
//      }
//    }
//  }

  // group by version
//  private def accuracyRuleInfos(ruleInfo: RuleInfo, expr: Expr, timeInfo: TimeInfo): Seq[RuleInfo] = {
//    val calcTime = timeInfo.calcTime
//    val details = ruleInfo.details
//    val sourceName = details.getString(AccuracyKeys._source, dataSourceNames.head)
//    val targetName = details.getString(AccuracyKeys._target, dataSourceNames.tail.head)
//    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)
//
//    if (!TempTables.existTable(timeInfo.key, sourceName)) {
//      Nil
//    } else {
//      // 1. miss record
//      val missRecordsSql = if (!TempTables.existTable(timeInfo.key, targetName)) {
//        val selClause = s"`${sourceName}`.*"
//        s"SELECT ${selClause} FROM `${sourceName}`"
//      } else {
//        val selClause = s"`${sourceName}`.*"
//        val onClause = expr.coalesceDesc
//        val sourceIsNull = analyzer.sourceSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val targetIsNull = analyzer.targetSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val whereClause = s"(NOT (${sourceIsNull})) AND (${targetIsNull})"
//        s"SELECT ${selClause} FROM `${sourceName}` LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
//      }
//      val missRecordsName = AccuracyKeys._missRecords
//      //      val tmstMissRecordsName = TempName.tmstName(missRecordsName, timeInfo)
//      val missRecordsParams = details.getParamMap(AccuracyKeys._missRecords)
//        .addIfNotExist(RuleDetailKeys._persistType, RecordPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, missRecordsName)
//      val missRecordsRuleInfo = RuleInfo(missRecordsName, None, SparkSqlType,
//        missRecordsSql, missRecordsParams, true)
//      //      val missRecordsStep = SparkSqlStep(
//      //        timeInfo,
//      //        RuleInfo(missRecordsName, Some(tmstMissRecordsName), missRecordsSql, missRecordsParams)
//      //      )
//
//      // 2. miss count
//      val missTableName = "_miss_"
//      //      val tmstMissTableName = TempName.tmstName(missTableName, timeInfo)
//      val missColName = details.getStringOrKey(AccuracyKeys._miss)
//      val missSql = {
//        s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${missColName}` FROM `${missRecordsName}` GROUP BY `${InternalColumns.tmst}`"
//      }
//      val missRuleInfo = RuleInfo(missTableName, None, SparkSqlType,
//        missSql, Map[String, Any](), true)
//      //      val missStep = SparkSqlStep(
//      //        timeInfo,
//      //        RuleInfo(missTableName, None, missSql, Map[String, Any]())
//      //      )
//
//      // 3. total count
//      val totalTableName = "_total_"
//      //      val tmstTotalTableName = TempName.tmstName(totalTableName, timeInfo)
//      val totalColName = details.getStringOrKey(AccuracyKeys._total)
//      val totalSql = {
//        s"SELECT `${InternalColumns.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceName}` GROUP BY `${InternalColumns.tmst}`"
//      }
//      val totalRuleInfo = RuleInfo(totalTableName, None, SparkSqlType,
//        totalSql, Map[String, Any](), true)
//      //      val totalStep = SparkSqlStep(
//      //        timeInfo,
//      //        RuleInfo(totalTableName, None, totalSql, Map[String, Any]())
//      //      )
//
//      // 4. accuracy metric
//      val accuracyMetricName = details.getString(RuleDetailKeys._persistName, ruleInfo.name)
//      //      val tmstAccuracyMetricName = TempName.tmstName(accuracyMetricName, timeInfo)
//      val matchedColName = details.getStringOrKey(AccuracyKeys._matched)
//      val accuracyMetricSql = {
//        s"""
//           |SELECT `${totalTableName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
//           |`${missTableName}`.`${missColName}` AS `${missColName}`,
//           |`${totalTableName}`.`${totalColName}` AS `${totalColName}`,
//           |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//           |FROM `${totalTableName}` FULL JOIN `${missTableName}`
//           |ON `${totalTableName}`.`${InternalColumns.tmst}` = `${missTableName}`.`${InternalColumns.tmst}`
//         """.stripMargin
//      }
//      //      val accuracyParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
////      val accuracyMetricRuleInfo = RuleInfo(accuracyMetricName, None, SparkSqlType,
////        accuracyMetricSql, Map[String, Any](), true)
//      val accuracyParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, accuracyMetricName)
//      val accuracyMetricRuleInfo = RuleInfo(accuracyMetricName, None, SparkSqlType,
//        accuracyMetricSql, Map[String, Any](), true)
//
//      // 5. accuracy metric merge
//      val globalMetricName = "accu_global"
//      val globalAccuSql = if (TempTables.existGlobalTable(globalMetricName)) {
//        s"""
//           |SELECT coalesce(`${globalMetricName}`.`${InternalColumns.tmst}`, `${accuracyMetricName}`.`${InternalColumns.tmst}`) AS `${InternalColumns.tmst}`,
//           |coalesce(`${accuracyMetricName}`.`${missColName}`, `${globalMetricName}`.`${missColName}`) AS `${missColName}`,
//           |coalesce(`${globalMetricName}`.`${totalColName}`, `${accuracyMetricName}`.`${totalColName}`) AS `${totalColName}`,
//           |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`,
//           |(`${totalColName}` = 0) AS `empty`,
//           |(`${missColName}` = 0) AS `no_miss`,
//           |(`${accuracyMetricName}`.`${missColName}` < `${globalMetricName}`.`${missColName}`) AS `update`
//           |FROM `${globalMetricName}` FULL JOIN `${accuracyMetricName}`
//           |ON `${globalMetricName}`.`${InternalColumns.tmst}` = `${accuracyMetricName}`.`${InternalColumns.tmst}`
//         """.stripMargin
//      } else {
//        s"""
//           |SELECT `${accuracyMetricName}`.`${InternalColumns.tmst}` AS `${InternalColumns.tmst}`,
//           |`${accuracyMetricName}`.`${missColName}` AS `${missColName}`,
//           |`${accuracyMetricName}`.`${totalColName}` AS `${totalColName}`,
//           |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`,
//           |(`${totalColName}` = 0) AS `empty`,
//           |(`${missColName}` = 0) AS `no_miss`,
//           |true AS `update`
//           |FROM `${accuracyMetricName}`
//         """.stripMargin
//      }
//      val globalAccuParams = Map[String, Any](
//        ("global" -> true)
//      )
//      val mergeRuleInfo = RuleInfo(globalMetricName, None, SparkSqlType,
//        globalAccuSql, globalAccuParams, true)
//
//      // 6. persist metrics
//      val persistMetricName = "persist"
//      val persistSql = {
//        s"""
//           |SELECT `${InternalColumns.tmst}`, `${missColName}`, `${totalColName}`, `${matchedColName}`
//           |FROM `${globalMetricName}`
//           |WHERE `update`
//         """.stripMargin
//      }
//      val persistParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, accuracyMetricName)
//      val persistRuleInfo = RuleInfo(persistMetricName, None, SparkSqlType,
//        persistSql, persistParams, true)
//
//      // 5. accuracy metric filter
////      val accuracyParams = details.addIfNotExist("df.name", accuracyMetricName)
////        .addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
////        .addIfNotExist(RuleDetailKeys._persistName, accuracyMetricName)
////      val accuracyRuleInfo = RuleInfo(accuracyMetricName, None, DfOprType,
////        "accuracy", accuracyParams, true)
//
////      missRecordsRuleInfo :: missRuleInfo :: totalRuleInfo ::
////        accuracyMetricRuleInfo :: accuracyRuleInfo :: Nil
//      missRecordsRuleInfo :: missRuleInfo :: totalRuleInfo ::
//        accuracyMetricRuleInfo :: mergeRuleInfo :: persistRuleInfo :: Nil
//    }
//  }

//  private def accuracyRuleInfos(ruleInfo: RuleInfo, expr: Expr, timeInfo: TimeInfo): Seq[RuleInfo] = {
//    val calcTime = timeInfo.calcTime
//    val details = ruleInfo.details
//    val sourceName = details.getString(AccuracyKeys._source, dataSourceNames.head)
//    val targetName = details.getString(AccuracyKeys._target, dataSourceNames.tail.head)
//    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)
//
//    if (!TempTables.existTable(timeInfo.key, sourceName)) {
//      Nil
//    } else {
//      // 1. miss record
//      val missRecordsSql = if (!TempTables.existTable(timeInfo.key, targetName)) {
//        val selClause = s"`${sourceName}`.*"
//        s"SELECT ${selClause} FROM `${sourceName}`"
//      } else {
//        val selClause = s"`${sourceName}`.*"
//        val onClause = expr.coalesceDesc
//        val sourceIsNull = analyzer.sourceSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val targetIsNull = analyzer.targetSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val whereClause = s"(NOT (${sourceIsNull})) AND (${targetIsNull})"
//        s"SELECT ${selClause} FROM `${sourceName}` LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
//      }
//      val missRecordsName = AccuracyKeys._missRecords
////      val tmstMissRecordsName = TempName.tmstName(missRecordsName, timeInfo)
//      val missRecordsParams = details.getParamMap(AccuracyKeys._missRecords)
//        .addIfNotExist(RuleDetailKeys._persistType, RecordPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, missRecordsName)
//      val missRecordsRuleInfo = RuleInfo(missRecordsName, None, SparkSqlType,
//        missRecordsSql, missRecordsParams, true)
////      val missRecordsStep = SparkSqlStep(
////        timeInfo,
////        RuleInfo(missRecordsName, Some(tmstMissRecordsName), missRecordsSql, missRecordsParams)
////      )
//
//      // 2. miss count
//      val missTableName = "_miss_"
//      //      val tmstMissTableName = TempName.tmstName(missTableName, timeInfo)
//      val missColName = details.getStringOrKey(AccuracyKeys._miss)
//      val missSql = {
//        s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsName}`"
//      }
//      val missRuleInfo = RuleInfo(missTableName, None, SparkSqlType,
//        missSql, Map[String, Any](), false)
////      val missStep = SparkSqlStep(
////        timeInfo,
////        RuleInfo(missTableName, None, missSql, Map[String, Any]())
////      )
//
//      // 3. total count
//      val totalTableName = "_total_"
//      //      val tmstTotalTableName = TempName.tmstName(totalTableName, timeInfo)
//      val totalColName = details.getStringOrKey(AccuracyKeys._total)
//      val totalSql = {
//        s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
//      }
//      val totalRuleInfo = RuleInfo(totalTableName, None, SparkSqlType,
//        totalSql, Map[String, Any](), false)
////      val totalStep = SparkSqlStep(
////        timeInfo,
////        RuleInfo(totalTableName, None, totalSql, Map[String, Any]())
////      )
//
//      // 4. accuracy metric
//      val accuracyMetricName = details.getString(RuleDetailKeys._persistName, ruleInfo.name)
////      val tmstAccuracyMetricName = TempName.tmstName(accuracyMetricName, timeInfo)
//      val matchedColName = details.getStringOrKey(AccuracyKeys._matched)
//      val accuracyMetricSql = {
//        s"""
//           |SELECT `${missTableName}`.`${missColName}` AS `${missColName}`,
//           |`${totalTableName}`.`${totalColName}` AS `${totalColName}`
//           |FROM `${totalTableName}` FULL JOIN `${missTableName}`
//         """.stripMargin
//      }
//      //      val accuracyParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//      val accuracyMetricRuleInfo = RuleInfo(accuracyMetricName, None, SparkSqlType,
//        accuracyMetricSql, Map[String, Any](), false)
////      val accuracyMetricStep = SparkSqlStep(
////        timeInfo,
////        RuleInfo(accuracyMetricName, Some(tmstAccuracyMetricName), accuracyMetricSql, Map[String, Any]())
////      )
//
//      // 5. accuracy metric filter
//      val accuracyParams = details.addIfNotExist("df.name", accuracyMetricName)
//        .addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, accuracyMetricName)
//      val accuracyRuleInfo = RuleInfo(accuracyMetricName, None, DfOprType,
//        "accuracy", accuracyParams, false)
////      val accuracyStep = DfOprStep(
////        timeInfo,
////        RuleInfo(accuracyMetricName, Some(tmstAccuracyMetricName), "accuracy", accuracyParams)
////      )
//
//      missRecordsRuleInfo :: missRuleInfo :: totalRuleInfo ::
//        accuracyMetricRuleInfo :: accuracyRuleInfo :: Nil
//    }
//  }

//  private def profilingRuleInfos(ruleInfo: RuleInfo, expr: Expr, timeInfo: TimeInfo): Seq[RuleInfo] = {
//    val details = ruleInfo.details
//    val profilingClause = expr.asInstanceOf[ProfilingClause]
//    val sourceName = profilingClause.fromClauseOpt match {
//      case Some(fc) => fc.dataSource
//      case _ => details.getString(ProfilingKeys._source, dataSourceNames.head)
//    }
//    val fromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc
//
//    if (!TempTables.existTable(timeInfo.key, sourceName)) {
//      Nil
//    } else {
//      val tmstAnalyzer = ProfilingAnalyzer(profilingClause, sourceName)
//
//      val selExprDescs = tmstAnalyzer.selectionExprs.map { sel =>
//        val alias = sel match {
//          case s: AliasableExpr if (s.alias.nonEmpty) => s" AS `${s.alias.get}`"
//          case _ => ""
//        }
//        s"${sel.desc}${alias}"
//      }
//      val selCondition = profilingClause.selectClause.extraConditionOpt.map(_.desc).mkString
//      val selClause = selExprDescs.mkString(", ")
////      val tmstFromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc
//      val groupByClauseOpt = tmstAnalyzer.groupbyExprOpt
//      val groupbyClause = groupByClauseOpt.map(_.desc).getOrElse("")
//      val preGroupbyClause = tmstAnalyzer.preGroupbyExprs.map(_.desc).mkString(" ")
//      val postGroupbyClause = tmstAnalyzer.postGroupbyExprs.map(_.desc).mkString(" ")
//
//      // 1. select statement
//      val profilingSql = {
//        s"SELECT ${selCondition} ${selClause} ${fromClause} ${preGroupbyClause} ${groupbyClause} ${postGroupbyClause}"
//      }
//      //      println(profilingSql)
//      val metricName = details.getString(RuleDetailKeys._persistName, ruleInfo.name)
//      //      val tmstMetricName = TempName.tmstName(metricName, timeInfo)
//      val profilingParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, metricName)
//      val profilingRuleInfo = ruleInfo.setDslType(SparkSqlType)
//        .setRule(profilingSql).setDetails(profilingParams)
////      val profilingStep = SparkSqlStep(
////        timeInfo,
////        ruleInfo.setRule(profilingSql).setDetails(profilingParams)
////      )
//
//      //      filterStep :: profilingStep :: Nil
//      profilingRuleInfo :: Nil
//    }
//  }

//  def genRuleStep(timeInfo: TimeInfo, param: Map[String, Any]): Seq[RuleStep] = {
//    val ruleInfo = RuleInfoGen(param, timeInfo)
//    val dqType = RuleInfoGen.dqType(param)
//    GriffinDslStep(timeInfo, ruleInfo, dqType) :: Nil
//  }
//
//  def adaptConcreteRuleStep(ruleStep: RuleStep
//                           ): Seq[ConcreteRuleStep] = {
//    ruleStep match {
//      case rs @ GriffinDslStep(_, ri, dqType) => {
//        try {
//          val result = parser.parseRule(ri.rule, dqType)
//          if (result.successful) {
//            val expr = result.get
//            transConcreteRuleStep(rs, expr)
//          } else {
//            println(result)
//            warn(s"adapt concrete rule step warn: parse rule [ ${ri.rule} ] fails")
//            Nil
//          }
//        } catch {
//          case e: Throwable => {
//            error(s"adapt concrete rule step error: ${e.getMessage}")
//            Nil
//          }
//        }
//      }
//      case _ => Nil
//    }
//  }
//
//  private def transConcreteRuleStep(ruleStep: GriffinDslStep, expr: Expr
//                                   ): Seq[ConcreteRuleStep] = {
//    ruleStep.dqType match {
//      case AccuracyType => transAccuracyRuleStep(ruleStep, expr)
//      case ProfilingType => transProfilingRuleStep(ruleStep, expr)
//      case TimelinessType => Nil
//      case _ => Nil
//    }
//  }

//  private def transAccuracyRuleStep(ruleStep: GriffinDslStep, expr: Expr
//                                   ): Seq[ConcreteRuleStep] = {
//    val timeInfo = ruleStep.timeInfo
//    val ruleInfo = ruleStep.ruleInfo
//    val calcTime = timeInfo.calcTime
//    val tmst = timeInfo.tmst
//
//    val details = ruleInfo.details
//    val sourceName = details.getString(AccuracyKeys._source, dataSourceNames.head)
//    val targetName = details.getString(AccuracyKeys._target, dataSourceNames.tail.head)
//    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)
//
//    if (!TempTables.existTable(key(calcTime), sourceName)) {
//      Nil
//    } else {
//      // 1. miss record
//      val missRecordsSql = if (!TempTables.existTable(key(calcTime), targetName)) {
//        val selClause = s"`${sourceName}`.*"
//        s"SELECT ${selClause} FROM `${sourceName}`"
//      } else {
//        val selClause = s"`${sourceName}`.*"
//        val onClause = expr.coalesceDesc
//        val sourceIsNull = analyzer.sourceSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val targetIsNull = analyzer.targetSelectionExprs.map { sel =>
//          s"${sel.desc} IS NULL"
//        }.mkString(" AND ")
//        val whereClause = s"(NOT (${sourceIsNull})) AND (${targetIsNull})"
//        s"SELECT ${selClause} FROM `${sourceName}` LEFT JOIN `${targetName}` ON ${onClause} WHERE ${whereClause}"
//      }
//      val missRecordsName = AccuracyKeys._missRecords
//      val tmstMissRecordsName = TempName.tmstName(missRecordsName, timeInfo)
//      val missRecordsParams = details.getParamMap(AccuracyKeys._missRecords)
//        .addIfNotExist(RuleDetailKeys._persistType, RecordPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, missRecordsName)
//      val missRecordsStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(missRecordsName, Some(tmstMissRecordsName), missRecordsSql, missRecordsParams)
//      )
//
//      // 2. miss count
//      val missTableName = "_miss_"
////      val tmstMissTableName = TempName.tmstName(missTableName, timeInfo)
//      val missColName = details.getStringOrKey(AccuracyKeys._miss)
//      val missSql = {
//        s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsName}`"
//      }
//      val missStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(missTableName, None, missSql, Map[String, Any]())
//      )
//
//      // 3. total count
//      val totalTableName = "_total_"
////      val tmstTotalTableName = TempName.tmstName(totalTableName, timeInfo)
//      val totalColName = details.getStringOrKey(AccuracyKeys._total)
//      val totalSql = {
//        s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
//      }
//      val totalStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(totalTableName, None, totalSql, Map[String, Any]())
//      )
//
//      // 4. accuracy metric
//      val accuracyMetricName = details.getString(RuleDetailKeys._persistName, ruleStep.name)
//      val tmstAccuracyMetricName = TempName.tmstName(accuracyMetricName, timeInfo)
//      val matchedColName = details.getStringOrKey(AccuracyKeys._matched)
//      val accuracyMetricSql = {
//        s"""
//           |SELECT `${missTableName}`.`${missColName}` AS `${missColName}`,
//           |`${totalTableName}`.`${totalColName}` AS `${totalColName}`
//           |FROM `${totalTableName}` FULL JOIN `${missTableName}`
//         """.stripMargin
//      }
////      val accuracyParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//      val accuracyMetricStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(accuracyMetricName, Some(tmstAccuracyMetricName), accuracyMetricSql, Map[String, Any]())
//      )
//
//      // 5. accuracy metric filter
//      val accuracyParams = details.addIfNotExist("df.name", accuracyMetricName)
//        .addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, accuracyMetricName)
//      val accuracyStep = DfOprStep(
//        timeInfo,
//        RuleInfo(accuracyMetricName, Some(tmstAccuracyMetricName), "accuracy", accuracyParams)
//      )
//
//      missRecordsStep :: missStep :: totalStep :: accuracyMetricStep :: accuracyStep :: Nil
//    }
//  }

//  private def transProfilingRuleStep(ruleStep: GriffinDslStep, expr: Expr
//                                    ): Seq[ConcreteRuleStep] = {
//    val calcTime = ruleStep.timeInfo.calcTime
//    val details = ruleStep.ruleInfo.details
//    val profilingClause = expr.asInstanceOf[ProfilingClause]
//    val sourceName = profilingClause.fromClauseOpt match {
//      case Some(fc) => fc.dataSource
//      case _ => details.getString(ProfilingKeys._source, dataSourceNames.head)
//    }
//    val fromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc
//
//    if (!TempTables.existTable(key(calcTime), sourceName)) {
//      Nil
//    } else {
//      val timeInfo = ruleStep.timeInfo
//      val ruleInfo = ruleStep.ruleInfo
//      val tmst = timeInfo.tmst
//
////      val tmstSourceName = TempName.tmstName(sourceName, timeInfo)
//
////      val tmstProfilingClause = profilingClause.map(dsHeadReplace(sourceName, tmstSourceName))
//      val tmstAnalyzer = ProfilingAnalyzer(profilingClause, sourceName)
//
//      val selExprDescs = tmstAnalyzer.selectionExprs.map { sel =>
//        val alias = sel match {
//          case s: AliasableExpr if (s.alias.nonEmpty) => s" AS `${s.alias.get}`"
//          case _ => ""
//        }
//        s"${sel.desc}${alias}"
//      }
//      val selCondition = profilingClause.selectClause.extraConditionOpt.map(_.desc).mkString
//      val selClause = selExprDescs.mkString(", ")
////      val tmstFromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc
//      val groupByClauseOpt = tmstAnalyzer.groupbyExprOpt
//      val groupbyClause = groupByClauseOpt.map(_.desc).getOrElse("")
//      val preGroupbyClause = tmstAnalyzer.preGroupbyExprs.map(_.desc).mkString(" ")
//      val postGroupbyClause = tmstAnalyzer.postGroupbyExprs.map(_.desc).mkString(" ")
//
//      // 1. select statement
//      val profilingSql = {
//        s"SELECT ${selCondition} ${selClause} ${fromClause} ${preGroupbyClause} ${groupbyClause} ${postGroupbyClause}"
//      }
////      println(profilingSql)
//      val metricName = details.getString(RuleDetailKeys._persistName, ruleStep.name)
////      val tmstMetricName = TempName.tmstName(metricName, timeInfo)
//      val profilingParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
//        .addIfNotExist(RuleDetailKeys._persistName, metricName)
//      val profilingStep = SparkSqlStep(
//        timeInfo,
//        ruleInfo.setRule(profilingSql).setDetails(profilingParams)
//      )
//
////      filterStep :: profilingStep :: Nil
//      profilingStep :: Nil
//    }
//
//  }

//  private def dsHeadReplace(originName: String, replaceName: String): (Expr) => Expr = { expr: Expr =>
//    expr match {
//      case DataSourceHeadExpr(sn) if (sn == originName) => {
//        DataSourceHeadExpr(replaceName)
//      }
//      case FromClause(sn) if (sn == originName) => {
//        FromClause(replaceName)
//      }
//      case _ => expr.map(dsHeadReplace(originName, replaceName))
//    }
//  }

}
