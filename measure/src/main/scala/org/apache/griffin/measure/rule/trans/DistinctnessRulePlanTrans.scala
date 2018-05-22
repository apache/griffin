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
import org.apache.griffin.measure.process._
import org.apache.griffin.measure.rule.adaptor.RuleParamKeys._
import org.apache.griffin.measure.rule.adaptor._
import org.apache.griffin.measure.rule.dsl.{ArrayCollectType, EntriesCollectType}
import org.apache.griffin.measure.rule.dsl.analyzer.DistinctnessAnalyzer
import org.apache.griffin.measure.rule.dsl.expr._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.rule.trans.RuleExportFactory._
import org.apache.griffin.measure.rule.trans.DsUpdateFactory._
import org.apache.griffin.measure.utils.ParamUtil._

import scala.util.Try

case class DistinctnessRulePlanTrans(dataSourceNames: Seq[String],
                                     timeInfo: TimeInfo, name: String, expr: Expr,
                                     param: Map[String, Any], procType: ProcessType,
                                     dsTimeRanges: Map[String, TimeRange]
                                    ) extends RulePlanTrans {

  private object DistinctnessKeys {
    val _source = "source"
    val _target = "target"
    val _distinct = "distinct"
    val _total = "total"
    val _dup = "dup"
    val _accu_dup = "accu_dup"
    val _num = "num"

    val _duplicationArray = "duplication.array"
    val _withAccumulate = "with.accumulate"

    val _recordEnable = "record.enable"
  }
  import DistinctnessKeys._

  def trans(): Try[RulePlan] = Try {
    val details = getDetails(param)
    val sourceName = details.getString(_source, dataSourceNames.head)
    val targetName = details.getString(_target, dataSourceNames.tail.head)
    val analyzer = DistinctnessAnalyzer(expr.asInstanceOf[DistinctnessClause], sourceName)

    val mode = SimpleMode

    val ct = timeInfo.calcTime

    val beginTmst = dsTimeRanges.get(sourceName).map(_.begin) match {
      case Some(t) => t
      case _ => throw new Exception(s"empty begin tmst from ${sourceName}")
    }
    val endTmst = dsTimeRanges.get(sourceName).map(_.end) match {
      case Some(t) => t
      case _ => throw new Exception(s"empty end tmst from ${sourceName}")
    }

    if (!TableRegisters.existRunTempTable(timeInfo.key, sourceName)) {
      println(s"[${ct}] data source ${sourceName} not exists")
      emptyRulePlan
    } else {
      val withOlderTable = {
        details.getBoolean(_withAccumulate, true) &&
          TableRegisters.existRunTempTable(timeInfo.key, targetName)
      }

      val selClause = analyzer.selectionPairs.map { pair =>
        val (expr, alias, _) = pair
        s"${expr.desc} AS `${alias}`"
      }.mkString(", ")
      val distAliases = analyzer.selectionPairs.filter(_._3).map(_._2)
      val distAliasesClause = distAliases.map( a => s"`${a}`" ).mkString(", ")
      val allAliases = analyzer.selectionPairs.map(_._2)
      val allAliasesClause = allAliases.map( a => s"`${a}`" ).mkString(", ")
      val groupAliases = analyzer.selectionPairs.filter(!_._3).map(_._2)
      val groupAliasesClause = groupAliases.map( a => s"`${a}`" ).mkString(", ")

      // 1. source alias
      val sourceAliasTableName = "__sourceAlias"
      val sourceAliasSql = {
        s"SELECT ${selClause} FROM `${sourceName}`"
      }
      val sourceAliasStep = SparkSqlStep(sourceAliasTableName, sourceAliasSql, emptyMap, true)

      // 2. total metric
      val totalTableName = "__totalMetric"
      val totalColName = details.getStringOrKey(_total)
      val totalSql = {
        s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceAliasTableName}`"
      }
      val totalStep = SparkSqlStep(totalTableName, totalSql, emptyMap)
      val totalMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
      val totalMetricExport = genMetricExport(totalMetricParam, totalColName, totalTableName, endTmst, mode)

      // 3. group by self
      val selfGroupTableName = "__selfGroup"
      val dupColName = details.getStringOrKey(_dup)
      val accuDupColName = details.getStringOrKey(_accu_dup)
      val selfGroupSql = {
        s"""
           |SELECT ${distAliasesClause}, (COUNT(*) - 1) AS `${dupColName}`,
           |TRUE AS `${InternalColumns.distinct}`
           |FROM `${sourceAliasTableName}` GROUP BY ${distAliasesClause}
          """.stripMargin
      }
      val selfGroupStep = SparkSqlStep(selfGroupTableName, selfGroupSql, emptyMap, true)

      val selfDistRulePlan = RulePlan(
        sourceAliasStep :: totalStep :: selfGroupStep :: Nil,
        totalMetricExport :: Nil
      )

      val (distRulePlan, dupCountTableName) = procType match {
        case StreamingProcessType if (withOlderTable) => {
          // 4.0 update old data
//          val updateOldTableName = "__updateOld"
//          val updateOldSql = {
//            s"SELECT * FROM `${targetName}`"
//          }
          val updateParam = emptyMap
          val targetDsUpdate = genDsUpdate(updateParam, targetName, targetName)

          // 4. older alias
          val olderAliasTableName = "__older"
          val olderAliasSql = {
            s"SELECT ${selClause} FROM `${targetName}` WHERE `${InternalColumns.tmst}` <= ${beginTmst}"
          }
          val olderAliasStep = SparkSqlStep(olderAliasTableName, olderAliasSql, emptyMap)

          // 5. join with older data
          val joinedTableName = "__joined"
          val selfSelClause = (distAliases :+ dupColName).map { alias =>
            s"`${selfGroupTableName}`.`${alias}`"
          }.mkString(", ")
          val onClause = distAliases.map { alias =>
            s"coalesce(`${selfGroupTableName}`.`${alias}`, '') = coalesce(`${olderAliasTableName}`.`${alias}`, '')"
          }.mkString(" AND ")
          val olderIsNull = distAliases.map { alias =>
            s"`${olderAliasTableName}`.`${alias}` IS NULL"
          }.mkString(" AND ")
          val joinedSql = {
            s"""
               |SELECT ${selfSelClause}, (${olderIsNull}) AS `${InternalColumns.distinct}`
               |FROM `${olderAliasTableName}` RIGHT JOIN `${selfGroupTableName}`
               |ON ${onClause}
            """.stripMargin
          }
          val joinedStep = SparkSqlStep(joinedTableName, joinedSql, emptyMap)

          // 6. group by joined data
          val groupTableName = "__group"
          val moreDupColName = "_more_dup"
          val groupSql = {
            s"""
               |SELECT ${distAliasesClause}, `${dupColName}`, `${InternalColumns.distinct}`,
               |COUNT(*) AS `${moreDupColName}`
               |FROM `${joinedTableName}`
               |GROUP BY ${distAliasesClause}, `${dupColName}`, `${InternalColumns.distinct}`
             """.stripMargin
          }
          val groupStep = SparkSqlStep(groupTableName, groupSql, emptyMap)

          // 7. final duplicate count
          val finalDupCountTableName = "__finalDupCount"
          // dupColName:      the duplicate count of duplicated items only occurs in new data,
          //                  which means the distinct one in new data is also duplicate
          // accuDupColName:  the count of duplicated items accumulated in new data and old data,
          //                  which means the accumulated distinct count in all data
          // e.g.:  new data [A, A, B, B, C, D], old data [A, A, B, C]
          //        selfGroupTable will be (A, 1, F), (B, 1, F), (C, 0, T), (D, 0, T)
          //        joinedTable will be (A, 1, F), (A, 1, F), (B, 1, F), (C, 0, F), (D, 0, T)
          //        groupTable will be (A, 1, F, 2), (B, 1, F, 1), (C, 0, F, 1), (D, 0, T, 1)
          //        finalDupCountTable will be (A, F, 2, 3), (B, F, 2, 2), (C, F, 1, 1), (D, T, 0, 0)
          //        The distinct result of new data only should be: (A, 2), (B, 2), (C, 1), (D, 0),
          //        which means in new data [A, A, B, B, C, D], [A, A, B, B, C] are all duplicated, only [D] is distinct
          val finalDupCountSql = {
            s"""
               |SELECT ${distAliasesClause}, `${InternalColumns.distinct}`,
               |CASE WHEN `${InternalColumns.distinct}` THEN `${dupColName}`
               |ELSE (`${dupColName}` + 1) END AS `${dupColName}`,
               |CASE WHEN `${InternalColumns.distinct}` THEN `${dupColName}`
               |ELSE (`${dupColName}` + `${moreDupColName}`) END AS `${accuDupColName}`
               |FROM `${groupTableName}`
             """.stripMargin
          }
          val finalDupCountStep = SparkSqlStep(finalDupCountTableName, finalDupCountSql, emptyMap, true)

          val rulePlan = RulePlan(
            olderAliasStep :: joinedStep :: groupStep :: finalDupCountStep :: Nil,
            Nil,
            targetDsUpdate :: Nil
          )
          (rulePlan, finalDupCountTableName)
        }
        case _ => {
          (emptyRulePlan, selfGroupTableName)
        }
      }

      // 8. distinct metric
      val distTableName = "__distMetric"
      val distColName = details.getStringOrKey(_distinct)
      val distSql = {
        s"""
           |SELECT COUNT(*) AS `${distColName}`
           |FROM `${dupCountTableName}` WHERE `${InternalColumns.distinct}`
         """.stripMargin
      }
      val distStep = SparkSqlStep(distTableName, distSql, emptyMap)
      val distMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, EntriesCollectType.desc)
      val distMetricExport = genMetricExport(distMetricParam, distColName, distTableName, endTmst, mode)

      val distMetricRulePlan = RulePlan(distStep :: Nil, distMetricExport :: Nil)

      val duplicationArrayName = details.getString(_duplicationArray, "")
      val dupRulePlan = if (duplicationArrayName.nonEmpty) {
        val recordEnable = details.getBoolean(_recordEnable, false)
        if (groupAliases.nonEmpty) {
          // with some group by requirement
          // 9. origin data join with distinct information
          val informedTableName = "__informed"
          val onClause = distAliases.map { alias =>
            s"coalesce(`${sourceAliasTableName}`.`${alias}`, '') = coalesce(`${dupCountTableName}`.`${alias}`, '')"
          }.mkString(" AND ")
          val informedSql = {
            s"""
               |SELECT `${sourceAliasTableName}`.*,
               |`${dupCountTableName}`.`${dupColName}` AS `${dupColName}`,
               |`${dupCountTableName}`.`${InternalColumns.distinct}` AS `${InternalColumns.distinct}`
               |FROM `${sourceAliasTableName}` LEFT JOIN `${dupCountTableName}`
               |ON ${onClause}
               """.stripMargin
          }
          val informedStep = SparkSqlStep(informedTableName, informedSql, emptyMap)

          // 10. add row number
          val rnTableName = "__rowNumber"
          val rnDistClause = distAliasesClause
          val rnSortClause = s"SORT BY `${InternalColumns.distinct}`"
          val rnSql = {
            s"""
               |SELECT *,
               |ROW_NUMBER() OVER (DISTRIBUTE BY ${rnDistClause} ${rnSortClause}) `${InternalColumns.rowNumber}`
               |FROM `${informedTableName}`
               """.stripMargin
          }
          val rnStep = SparkSqlStep(rnTableName, rnSql, emptyMap)

          // 11. recognize duplicate items
          val dupItemsTableName = "__dupItems"
          val dupItemsSql = {
            s"""
               |SELECT ${allAliasesClause}, `${dupColName}` FROM `${rnTableName}`
               |WHERE NOT `${InternalColumns.distinct}` OR `${InternalColumns.rowNumber}` > 1
               """.stripMargin
          }
          val dupItemsStep = SparkSqlStep(dupItemsTableName, dupItemsSql, emptyMap)
          val dupItemsParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          val dupItemsExport = genRecordExport(dupItemsParam, dupItemsTableName, dupItemsTableName, endTmst, mode)

          // 12. group by dup Record metric
          val groupDupMetricTableName = "__groupDupMetric"
          val numColName = details.getStringOrKey(_num)
          val groupSelClause = groupAliasesClause
          val groupDupMetricSql = {
            s"""
               |SELECT ${groupSelClause}, `${dupColName}`, COUNT(*) AS `${numColName}`
               |FROM `${dupItemsTableName}` GROUP BY ${groupSelClause}, `${dupColName}`
             """.stripMargin
          }
          val groupDupMetricStep = SparkSqlStep(groupDupMetricTableName, groupDupMetricSql, emptyMap)
          val groupDupMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, ArrayCollectType.desc)
          val groupDupMetricExport = genMetricExport(groupDupMetricParam, duplicationArrayName, groupDupMetricTableName, endTmst, mode)

          val exports = if (recordEnable) {
            dupItemsExport :: groupDupMetricExport :: Nil
          } else {
            groupDupMetricExport :: Nil
          }
          RulePlan(
            informedStep :: rnStep :: dupItemsStep :: groupDupMetricStep :: Nil,
            exports
          )

        } else {
          // no group by requirement
          // 9. duplicate record
          val dupRecordTableName = "__dupRecords"
          val dupRecordSelClause = procType match {
            case StreamingProcessType if (withOlderTable) => s"${distAliasesClause}, `${dupColName}`, `${accuDupColName}`"
            case _ => s"${distAliasesClause}, `${dupColName}`"
          }
          val dupRecordSql = {
            s"""
               |SELECT ${dupRecordSelClause}
               |FROM `${dupCountTableName}` WHERE `${dupColName}` > 0
              """.stripMargin
          }
          val dupRecordStep = SparkSqlStep(dupRecordTableName, dupRecordSql, emptyMap, true)
          val dupRecordParam = RuleParamKeys.getRecordOpt(param).getOrElse(emptyMap)
          val dupRecordExport = genRecordExport(dupRecordParam, dupRecordTableName, dupRecordTableName, endTmst, mode)

          // 10. duplicate metric
          val dupMetricTableName = "__dupMetric"
          val numColName = details.getStringOrKey(_num)
          val dupMetricSql = {
            s"""
               |SELECT `${dupColName}`, COUNT(*) AS `${numColName}`
               |FROM `${dupRecordTableName}` GROUP BY `${dupColName}`
              """.stripMargin
          }
          val dupMetricStep = SparkSqlStep(dupMetricTableName, dupMetricSql, emptyMap)
          val dupMetricParam = emptyMap.addIfNotExist(ExportParamKeys._collectType, ArrayCollectType.desc)
          val dupMetricExport = genMetricExport(dupMetricParam, duplicationArrayName, dupMetricTableName, endTmst, mode)

          val exports = if (recordEnable) {
            dupRecordExport :: dupMetricExport :: Nil
          } else {
            dupMetricExport :: Nil
          }
          RulePlan(dupRecordStep :: dupMetricStep :: Nil, exports)
        }
      } else emptyRulePlan

      selfDistRulePlan.merge(distRulePlan).merge(distMetricRulePlan).merge(dupRulePlan)

    }
  }

}
