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

import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.dsl.analyzer._
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.rule.step._
import org.apache.griffin.measure.utils.ParamUtil._

case class GriffinDslAdaptor(dataSourceNames: Seq[String],
                             functionNames: Seq[String],
                             adaptPhase: AdaptPhase
                            ) extends RuleAdaptor {

  object StepInfo {
    val _Name = "name"
    val _PersistType = "persist.type"
    val _UpdateDataSource = "update.data.source"
    def getNameOpt(param: Map[String, Any]): Option[String] = param.get(_Name).map(_.toString)
    def getPersistType(param: Map[String, Any]): PersistType = PersistType(param.getString(_PersistType, ""))
    def getUpdateDataSourceOpt(param: Map[String, Any]): Option[String] = param.get(_UpdateDataSource).map(_.toString)
  }
  object AccuracyInfo {
    val _Source = "source"
    val _Target = "target"
    val _MissRecords = "miss.records"
    val _Accuracy = "accuracy"
    val _Miss = "miss"
    val _Total = "total"
    val _Matched = "matched"
  }
  object ProfilingInfo {
    val _Source = "source"
    val _Profiling = "profiling"
  }

  def getNameOpt(param: Map[String, Any], key: String): Option[String] = param.get(key).map(_.toString)
  def resultName(param: Map[String, Any], key: String): String = {
    val nameOpt = param.get(key) match {
      case Some(prm: Map[String, Any]) => StepInfo.getNameOpt(prm)
      case _ => None
    }
    nameOpt.getOrElse(key)
  }
  def resultPersistType(param: Map[String, Any], key: String, defPersistType: PersistType): PersistType = {
    param.get(key) match {
      case Some(prm: Map[String, Any]) => StepInfo.getPersistType(prm)
      case _ => defPersistType
    }
  }
  def resultUpdateDataSourceOpt(param: Map[String, Any], key: String): Option[String] = {
    param.get(key) match {
      case Some(prm: Map[String, Any]) => StepInfo.getUpdateDataSourceOpt(prm)
      case _ => None
    }
  }

  val _dqType = "dq.type"

  protected def getDqType(param: Map[String, Any]) = DqType(param.getString(_dqType, ""))

  val filteredFunctionNames = functionNames.filter { fn =>
    fn.matches("""^[a-zA-Z_]\w*$""")
  }
  val parser = GriffinDslParser(dataSourceNames, filteredFunctionNames)

  def genRuleStep(param: Map[String, Any]): Seq[RuleStep] = {
    GriffinDslStep(getName(param), getRule(param), getDqType(param), getDetails(param)) :: Nil
  }

  def getTempSourceNames(param: Map[String, Any]): Seq[String] = {
    val dqType = getDqType(param)
    param.get(_name) match {
      case Some(name) => {
        dqType match {
          case AccuracyType => {
            Seq[String](
              resultName(param, AccuracyInfo._MissRecords),
              resultName(param, AccuracyInfo._Accuracy)
            )
          }
          case ProfilingType => {
            Seq[String](
              resultName(param, ProfilingInfo._Profiling)
            )
          }
          case TimelinessType => {
            Nil
          }
          case _ => Nil
        }
      }
      case _ => Nil
    }
  }

  def adaptConcreteRuleStep(ruleStep: RuleStep): Seq[ConcreteRuleStep] = {
    ruleStep match {
      case rs @ GriffinDslStep(_, rule, dqType, _) => {
        val exprOpt = try {
          val result = parser.parseRule(rule, dqType)
          if (result.successful) Some(result.get)
          else {
            println(result)
            warn(s"adapt concrete rule step warn: parse rule [ ${rule} ] fails")
            None
          }
        } catch {
          case e: Throwable => {
            error(s"adapt concrete rule step error: ${e.getMessage}")
            None
          }
        }

        exprOpt match {
          case Some(expr) => {
            try {
              transConcreteRuleSteps(rs, expr)
            } catch {
              case e: Throwable => {
                error(s"trans concrete rule step error: ${e.getMessage}")
                Nil
              }
            }
          }
          case _ => Nil
        }
      }
      case _ => Nil
    }
  }

  private def transConcreteRuleSteps(ruleStep: GriffinDslStep, expr: Expr
                                    ): Seq[ConcreteRuleStep] = {
    val details = ruleStep.details
    ruleStep.dqType match {
      case AccuracyType => {
        val sourceName = getNameOpt(details, AccuracyInfo._Source) match {
          case Some(name) => name
          case _ => dataSourceNames.head
        }
        val targetName = getNameOpt(details, AccuracyInfo._Target) match {
          case Some(name) => name
          case _ => dataSourceNames.tail.head
        }
        val analyzer = AccuracyAnalyzer(expr.asInstanceOf[LogicalExpr], sourceName, targetName)


        if (!checkDataSourceExists(sourceName)) {
          Nil
        } else {
          // 1. miss record
          val missRecordsSql = if (!checkDataSourceExists(targetName)) {
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
          val missRecordsName = resultName(details, AccuracyInfo._MissRecords)
          val missRecordsStep = SparkSqlStep(
            missRecordsName,
            missRecordsSql,
            Map[String, Any](),
            resultPersistType(details, AccuracyInfo._MissRecords, RecordPersistType),
            resultUpdateDataSourceOpt(details, AccuracyInfo._MissRecords)
          )

          // 2. miss count
          val missTableName = "_miss_"
          val missColName = getNameOpt(details, AccuracyInfo._Miss).getOrElse(AccuracyInfo._Miss)
          val missSql = {
            s"SELECT `${GroupByColumn.tmst}` AS `${GroupByColumn.tmst}`, COUNT(*) AS `${missColName}` FROM `${missRecordsName}` GROUP BY `${GroupByColumn.tmst}`"
          }
          val missStep = SparkSqlStep(
            missTableName,
            missSql,
            Map[String, Any](),
            NonePersistType,
            None
          )

          // 3. total count
          val totalTableName = "_total_"
          val totalColName = getNameOpt(details, AccuracyInfo._Total).getOrElse(AccuracyInfo._Total)
          val totalSql = {
            s"SELECT `${GroupByColumn.tmst}` AS `${GroupByColumn.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceName}` GROUP BY `${GroupByColumn.tmst}`"
          }
          val totalStep = SparkSqlStep(
            totalTableName,
            totalSql,
            Map[String, Any](),
            NonePersistType,
            None
          )

          // 4. accuracy metric
          val matchedColName = getNameOpt(details, AccuracyInfo._Matched).getOrElse(AccuracyInfo._Matched)
          val accuracyMetricSql = {
            s"""
               |SELECT `${totalTableName}`.`${GroupByColumn.tmst}` AS `${GroupByColumn.tmst}`,
               |`${missTableName}`.`${missColName}` AS `${missColName}`,
               |`${totalTableName}`.`${totalColName}` AS `${totalColName}`
               |FROM `${totalTableName}` FULL JOIN `${missTableName}`
               |ON `${totalTableName}`.`${GroupByColumn.tmst}` = `${missTableName}`.`${GroupByColumn.tmst}`
          """.stripMargin
          }
          val accuracyMetricName = resultName(details, AccuracyInfo._Accuracy)
          val accuracyMetricStep = SparkSqlStep(
            accuracyMetricName,
            accuracyMetricSql,
            details,
            //          resultPersistType(details, AccuracyInfo._Accuracy, MetricPersistType)
            NonePersistType,
            None
          )

          // 5. accuracy metric filter
          val accuracyStep = DfOprStep(
            accuracyMetricName,
            "accuracy",
            Map[String, Any](
              ("df.name" -> accuracyMetricName),
              ("miss" -> missColName),
              ("total" -> totalColName),
              ("matched" -> matchedColName),
              ("tmst" -> GroupByColumn.tmst)
            ),
            resultPersistType(details, AccuracyInfo._Accuracy, MetricPersistType),
            None
          )

          missRecordsStep :: missStep :: totalStep :: accuracyMetricStep :: accuracyStep :: Nil
        }
      }
      case ProfilingType => {
        val profilingClause = expr.asInstanceOf[ProfilingClause]
        val sourceName = profilingClause.fromClauseOpt match {
          case Some(fc) => fc.dataSource
          case _ => {
            getNameOpt(details, ProfilingInfo._Source) match {
              case Some(name) => name
              case _ => dataSourceNames.head
            }
          }
        }
        val analyzer = ProfilingAnalyzer(profilingClause, sourceName)

//        analyzer.selectionExprs.foreach(println)

        val selExprDescs = analyzer.selectionExprs.map { sel =>
          val alias = sel match {
            case s: AliasableExpr if (s.alias.nonEmpty) => s" AS `${s.alias.get}`"
            case _ => ""
          }
          s"${sel.desc}${alias}"
        }

//        val selClause = (s"`${GroupByColumn.tmst}`" +: selExprDescs).mkString(", ")
        val selClause = if (analyzer.containsAllSelectionExpr) {
          selExprDescs.mkString(", ")
        } else {
          (s"`${GroupByColumn.tmst}`" +: selExprDescs).mkString(", ")
        }

        val fromClause = profilingClause.fromClauseOpt.getOrElse(FromClause(sourceName)).desc

//        val tailClause = analyzer.tailsExprs.map(_.desc).mkString(" ")
        val tmstGroupbyClause = GroupbyClause(LiteralStringExpr(s"`${GroupByColumn.tmst}`") :: Nil, None)
        val mergedGroubbyClause = tmstGroupbyClause.merge(analyzer.groupbyExprOpt match {
          case Some(gbc) => gbc
          case _ => GroupbyClause(Nil, None)
        })
        val groupbyClause = mergedGroubbyClause.desc
        val preGroupbyClause = analyzer.preGroupbyExprs.map(_.desc).mkString(" ")
        val postGroupbyClause = analyzer.postGroupbyExprs.map(_.desc).mkString(" ")

        if (!checkDataSourceExists(sourceName)) {
          Nil
        } else {
          // 1. select statement
          val profilingSql = {
//            s"SELECT `${GroupByColumn.tmst}`, ${selClause} FROM ${sourceName} ${tailClause} GROUP BY `${GroupByColumn.tmst}`"
            s"SELECT ${selClause} ${fromClause} ${preGroupbyClause} ${groupbyClause} ${postGroupbyClause}"
          }
          val profilingMetricName = resultName(details, ProfilingInfo._Profiling)
          val profilingStep = SparkSqlStep(
            profilingMetricName,
            profilingSql,
            details,
            resultPersistType(details, ProfilingInfo._Profiling, MetricPersistType),
            None
          )

          // 2. clear processed data
//          val clearDataSourceStep = DfOprStep(
//            s"${sourceName}_clear",
//            "clear",
//            Map[String, Any](
//              ("df.name" -> sourceName)
//            ),
//            NonePersistType,
//            Some(sourceName)
//          )
//
//          profilingStep :: clearDataSourceStep :: Nil

          profilingStep:: Nil
        }

      }
      case TimelinessType => {
        Nil
      }
      case _ => Nil
    }
  }

  private def checkDataSourceExists(name: String): Boolean = {
    try {
      RuleAdaptorGroup.dataChecker.existDataSourceName(name)
    } catch {
      case e: Throwable => {
        error(s"check data source exists error: ${e.getMessage}")
        false
      }
    }
  }

}
