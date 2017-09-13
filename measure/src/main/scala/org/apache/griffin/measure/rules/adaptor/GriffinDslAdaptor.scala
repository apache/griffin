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
package org.apache.griffin.measure.rules.adaptor

import org.apache.griffin.measure.algo.{BatchProcessType, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.rules.dsl._
import org.apache.griffin.measure.rules.dsl.analyzer._
import org.apache.griffin.measure.rules.dsl.expr._
import org.apache.griffin.measure.rules.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.rules.step._

case class GriffinDslAdaptor(dataSourceNames: Seq[String],
                             functionNames: Seq[String],
                             adaptPhase: AdaptPhase
                            ) extends RuleAdaptor {

  object StepInfo {
    val _Name = "name"
    val _PersistType = "persist.type"
    val _UpdateDataSource = "update.data.source"
    def getNameOpt(param: Map[String, Any]): Option[String] = param.get(_Name).map(_.toString)
    def getPersistType(param: Map[String, Any]): PersistType = PersistType(param.getOrElse(_PersistType, "").toString)
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

  protected def getDqType(param: Map[String, Any]) = DqType(param.getOrElse(_dqType, "").toString)

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

  private def transConcreteRuleSteps(ruleStep: GriffinDslStep, expr: Expr): Seq[ConcreteRuleStep] = {
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

        // 1. miss record
        val missRecordsSql = {
//          val selClause = analyzer.selectionExprs.map { sel =>
//            val alias = sel.alias match {
//              case Some(a) => s" AS ${a}"
//              case _ => ""
//            }
//            s"${sel.desc}${alias}"
//          }.mkString(", ")
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
//        val missSql = processType match {
//          case BatchProcessType => {
//            s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsName}`"
//          }
//          case StreamingProcessType => {
//            s"SELECT `${GroupByColumn.tmst}`, COUNT(*) AS `${missColName}` FROM `${missRecordsName}` GROUP BY `${GroupByColumn.tmst}`"
//          }
//        }
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
//        val totalSql = processType match {
//          case BatchProcessType => {
//            s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
//          }
//          case StreamingProcessType => {
//            s"SELECT `${GroupByColumn.tmst}`, COUNT(*) AS `${totalColName}` FROM `${sourceName}` GROUP BY `${GroupByColumn.tmst}`"
//          }
//        }
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
//        val accuracyMetricSql = processType match {
//          case BatchProcessType => {
//            s"""
//               |SELECT `${totalTableName}`.`${totalColName}` AS `${totalColName}`,
//               |`${missTableName}`.`${missColName}` AS `${missColName}`,
//               |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//               |FROM `${totalTableName}` JOIN `${missTableName}`
//            """.stripMargin
//          }
//          case StreamingProcessType => {
//            s"""
//               |SELECT `${missTableName}`.`${GroupByColumn.tmst}`,
//               |`${totalTableName}`.`${totalColName}` AS `${totalColName}`,
//               |`${missTableName}`.`${missColName}` AS `${missColName}`,
//               |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//               |FROM `${totalTableName}` JOIN `${missTableName}`
//               |ON `${totalTableName}`.`${GroupByColumn.tmst}` = `${missTableName}`.`${GroupByColumn.tmst}`
//            """.stripMargin
//          }
//        }


//        val accuracyMetricSql = {
//          s"""
//             |SELECT `${missTableName}`.`${GroupByColumn.tmst}`,
//             |`${totalTableName}`.`${totalColName}` AS `${totalColName}`,
//             |`${missTableName}`.`${missColName}` AS `${missColName}`,
//             |(`${totalColName}` - `${missColName}`) AS `${matchedColName}`
//             |FROM `${totalTableName}` JOIN `${missTableName}`
//             |ON `${totalTableName}`.`${GroupByColumn.tmst}` = `${missTableName}`.`${GroupByColumn.tmst}`
//          """.stripMargin
//        }
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
      case ProfilingType => {
        val sourceName = getNameOpt(details, ProfilingInfo._Source) match {
          case Some(name) => name
          case _ => dataSourceNames.head
        }
        val analyzer = ProfilingAnalyzer(expr.asInstanceOf[CombinedClause], sourceName)

        val selClause = analyzer.selectionExprs.map { sel =>
          val alias = sel match {
            case s: AliasableExpr if (s.alias.nonEmpty) => s" AS ${s.alias.get}"
            case _ => ""
          }
          s"${sel.desc}${alias}"
        }.mkString(", ")

        val tailClause = analyzer.tailsExprs.map(_.desc).mkString(" ")

        // 1. select statement
//        val profilingSql = processType match {
//          case BatchProcessType => {
//            s"SELECT ${selClause} FROM ${sourceName} ${tailClause}"
//          }
//          case StreamingProcessType => {
//            s"SELECT ${GroupByColumn.tmst}, ${selClause} FROM ${sourceName} ${tailClause}" +
//              s" GROUP BY ${GroupByColumn.tmst}"
//          }
//        }
        val profilingSql = {
          s"SELECT `${GroupByColumn.tmst}`, ${selClause} FROM ${sourceName} ${tailClause} GROUP BY `${GroupByColumn.tmst}`"
        }
        val profilingMetricName = resultName(details, ProfilingInfo._Profiling)
        val profilingStep = SparkSqlStep(
          profilingMetricName,
          profilingSql,
          details,
          resultPersistType(details, ProfilingInfo._Profiling, MetricPersistType),
          None
        )

        profilingStep :: Nil
      }
      case TimelinessType => {
        Nil
      }
      case _ => Nil
    }
  }

}
