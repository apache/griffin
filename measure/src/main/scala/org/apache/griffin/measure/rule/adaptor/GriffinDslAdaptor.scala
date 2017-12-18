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
import org.apache.griffin.measure.data.connector.InternalColumns
import org.apache.griffin.measure.process.temp.TempTables
import org.apache.griffin.measure.process.{BatchProcessType, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.dsl.analyzer._
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.rule.step._
import org.apache.griffin.measure.utils.ParamUtil._

case class GriffinDslAdaptor(dataSourceNames: Seq[String],
                             functionNames: Seq[String]
                            ) extends RuleAdaptor {

  object AccuracyKeys {
    val _source = "source"
    val _target = "target"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
    val _missRecords = "missRecords"
  }
  object ProfilingKeys {
    val _source = "source"
  }

  val filteredFunctionNames = functionNames.filter { fn =>
    fn.matches("""^[a-zA-Z_]\w*$""")
  }
  val parser = GriffinDslParser(dataSourceNames, filteredFunctionNames)

  override def genRuleInfos(param: Map[String, Any], timeInfo: TimeInfo): Seq[RuleInfo] = {
    val ruleInfo = RuleInfoGen(param)
    val dqType = RuleInfoGen.dqType(param)
    try {
      val result = parser.parseRule(ruleInfo.rule, dqType)
      if (result.successful) {
        val expr = result.get
        dqType match {
          case AccuracyType => accuracyRuleInfos(ruleInfo, expr, timeInfo)
          case ProfilingType => profilingRuleInfos(ruleInfo, expr, timeInfo)
          case TimelinessType => Nil
          case _ => Nil
        }
      } else {
        warn(s"parse rule [ ${ruleInfo.rule} ] fails: \n${result}")
        Nil
      }
    } catch {
      case e: Throwable => {
        error(s"generate rule info ${ruleInfo} fails: ${e.getMessage}")
        Nil
      }
    }
  }

  private def accuracyRuleInfos(ruleInfo: RuleInfo, expr: Expr, timeInfo: TimeInfo): Seq[RuleInfo] = {
    val calcTime = timeInfo.calcTime
    val details = ruleInfo.details
    val sourceName = details.getString(AccuracyKeys._source, dataSourceNames.head)
    val targetName = details.getString(AccuracyKeys._target, dataSourceNames.tail.head)
    val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)

    if (!TempTables.existTable(timeInfo.key, sourceName)) {
      Nil
    } else {
      // 1. miss record
      val missRecordsSql = if (!TempTables.existTable(timeInfo.key, targetName)) {
        val selClause = s"`${sourceName}`.*"
        s"SELECT ${selClause} FROM `${sourceName}`"
      } else {
        val selClause = s"`${sourceName}`.*"
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
      val missRecordsName = AccuracyKeys._missRecords
//      val tmstMissRecordsName = TempName.tmstName(missRecordsName, timeInfo)
      val missRecordsParams = details.getParamMap(AccuracyKeys._missRecords)
        .addIfNotExist(RuleDetailKeys._persistType, RecordPersistType.desc)
        .addIfNotExist(RuleDetailKeys._persistName, missRecordsName)
      val missRecordsRuleInfo = RuleInfo(missRecordsName, None, SparkSqlType,
        missRecordsSql, missRecordsParams, true)
//      val missRecordsStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(missRecordsName, Some(tmstMissRecordsName), missRecordsSql, missRecordsParams)
//      )

      // 2. miss count
      val missTableName = "_miss_"
      //      val tmstMissTableName = TempName.tmstName(missTableName, timeInfo)
      val missColName = details.getStringOrKey(AccuracyKeys._miss)
      val missSql = {
        s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsName}`"
      }
      val missRuleInfo = RuleInfo(missTableName, None, SparkSqlType,
        missSql, Map[String, Any](), false)
//      val missStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(missTableName, None, missSql, Map[String, Any]())
//      )

      // 3. total count
      val totalTableName = "_total_"
      //      val tmstTotalTableName = TempName.tmstName(totalTableName, timeInfo)
      val totalColName = details.getStringOrKey(AccuracyKeys._total)
      val totalSql = {
        s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
      }
      val totalRuleInfo = RuleInfo(totalTableName, None, SparkSqlType,
        totalSql, Map[String, Any](), false)
//      val totalStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(totalTableName, None, totalSql, Map[String, Any]())
//      )

      // 4. accuracy metric
      val accuracyMetricName = details.getString(RuleDetailKeys._persistName, ruleInfo.name)
//      val tmstAccuracyMetricName = TempName.tmstName(accuracyMetricName, timeInfo)
      val matchedColName = details.getStringOrKey(AccuracyKeys._matched)
      val accuracyMetricSql = {
        s"""
           |SELECT `${missTableName}`.`${missColName}` AS `${missColName}`,
           |`${totalTableName}`.`${totalColName}` AS `${totalColName}`
           |FROM `${totalTableName}` FULL JOIN `${missTableName}`
         """.stripMargin
      }
      //      val accuracyParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
      val accuracyMetricRuleInfo = RuleInfo(accuracyMetricName, None, SparkSqlType,
        accuracyMetricSql, Map[String, Any](), false)
//      val accuracyMetricStep = SparkSqlStep(
//        timeInfo,
//        RuleInfo(accuracyMetricName, Some(tmstAccuracyMetricName), accuracyMetricSql, Map[String, Any]())
//      )

      // 5. accuracy metric filter
      val accuracyParams = details.addIfNotExist("df.name", accuracyMetricName)
        .addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
        .addIfNotExist(RuleDetailKeys._persistName, accuracyMetricName)
      val accuracyRuleInfo = RuleInfo(accuracyMetricName, None, DfOprType,
        "accuracy", accuracyParams, false)
//      val accuracyStep = DfOprStep(
//        timeInfo,
//        RuleInfo(accuracyMetricName, Some(tmstAccuracyMetricName), "accuracy", accuracyParams)
//      )

      missRecordsRuleInfo :: missRuleInfo :: totalRuleInfo ::
        accuracyMetricRuleInfo :: accuracyRuleInfo :: Nil
    }
  }
  private def profilingRuleInfos(ruleInfo: RuleInfo, expr: Expr, timeInfo: TimeInfo): Seq[RuleInfo] = {
    val details = ruleInfo.details
    val profilingClause = expr.asInstanceOf[ProfilingClause]
    val sourceName = profilingClause.fromClauseOpt match {
      case Some(fc) => fc.dataSource
      case _ => details.getString(ProfilingKeys._source, dataSourceNames.head)
    }
    val fromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc

    if (!TempTables.existTable(timeInfo.key, sourceName)) {
      Nil
    } else {
      val tmstAnalyzer = ProfilingAnalyzer(profilingClause, sourceName)

      val selExprDescs = tmstAnalyzer.selectionExprs.map { sel =>
        val alias = sel match {
          case s: AliasableExpr if (s.alias.nonEmpty) => s" AS `${s.alias.get}`"
          case _ => ""
        }
        s"${sel.desc}${alias}"
      }
      val selCondition = profilingClause.selectClause.extraConditionOpt.map(_.desc).mkString
      val selClause = selExprDescs.mkString(", ")
//      val tmstFromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc
      val groupByClauseOpt = tmstAnalyzer.groupbyExprOpt
      val groupbyClause = groupByClauseOpt.map(_.desc).getOrElse("")
      val preGroupbyClause = tmstAnalyzer.preGroupbyExprs.map(_.desc).mkString(" ")
      val postGroupbyClause = tmstAnalyzer.postGroupbyExprs.map(_.desc).mkString(" ")

      // 1. select statement
      val profilingSql = {
        s"SELECT ${selCondition} ${selClause} ${fromClause} ${preGroupbyClause} ${groupbyClause} ${postGroupbyClause}"
      }
      //      println(profilingSql)
      val metricName = details.getString(RuleDetailKeys._persistName, ruleInfo.name)
      //      val tmstMetricName = TempName.tmstName(metricName, timeInfo)
      val profilingParams = details.addIfNotExist(RuleDetailKeys._persistType, MetricPersistType.desc)
        .addIfNotExist(RuleDetailKeys._persistName, metricName)
      val profilingRuleInfo = ruleInfo.setDslType(SparkSqlType)
        .setRule(profilingSql).setDetails(profilingParams)
//      val profilingStep = SparkSqlStep(
//        timeInfo,
//        ruleInfo.setRule(profilingSql).setDetails(profilingParams)
//      )

      //      filterStep :: profilingStep :: Nil
      profilingRuleInfo :: Nil
    }
  }

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
