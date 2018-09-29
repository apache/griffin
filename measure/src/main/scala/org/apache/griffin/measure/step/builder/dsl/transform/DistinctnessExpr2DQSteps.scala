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
package org.apache.griffin.measure.step.builder.dsl.transform

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.step.builder.dsl.expr.{DistinctnessClause, _}
import org.apache.griffin.measure.step.builder.dsl.transform.analyzer.DistinctnessAnalyzer
import org.apache.griffin.measure.step.transform.SparkSqlTransformStep
import org.apache.griffin.measure.step.write.{DataSourceUpdateWriteStep, MetricWriteStep, RecordWriteStep}
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * generate distinctness dq steps
  */
case class DistinctnessExpr2DQSteps(context: DQContext,
                                    expr: Expr,
                                    ruleParam: RuleParam
                                   ) extends Expr2DQSteps {

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

  def getDQSteps(): Seq[DQStep] = {
    val details = ruleParam.getDetails
    val distinctnessExpr = expr.asInstanceOf[DistinctnessClause]

    val sourceName = details.getString(_source, context.getDataSourceName(0))
    val targetName = details.getString(_target, context.getDataSourceName(1))
    val analyzer = DistinctnessAnalyzer(distinctnessExpr, sourceName)

    val procType = context.procType
    val timestamp = context.contextId.timestamp
    val dsTimeRanges = context.dataSourceTimeRanges

    val beginTmst = dsTimeRanges.get(sourceName).map(_.begin) match {
      case Some(t) => t
      case _ => throw new Exception(s"empty begin tmst from ${sourceName}")
    }
    val endTmst = dsTimeRanges.get(sourceName).map(_.end) match {
      case Some(t) => t
      case _ => throw new Exception(s"empty end tmst from ${sourceName}")
    }

    val writeTimestampOpt = Some(endTmst)

    if (!context.runTimeTableRegister.existsTable(sourceName)) {
      warn(s"[${timestamp}] data source ${sourceName} not exists")
      Nil
    } else {
      val withOlderTable = {
        details.getBoolean(_withAccumulate, true) &&
          context.runTimeTableRegister.existsTable(targetName)
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
      val sourceAliasTransStep =
        SparkSqlTransformStep(sourceAliasTableName, sourceAliasSql, emptyMap, true)

      // 2. total metric
      val totalTableName = "__totalMetric"
      val totalColName = details.getStringOrKey(_total)
      val totalSql = {
        s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceAliasTableName}`"
      }
      val totalTransStep = SparkSqlTransformStep(totalTableName, totalSql, emptyMap)
      val totalMetricWriteStep = {
        MetricWriteStep(totalColName, totalTableName, EntriesFlattenType, writeTimestampOpt)
      }

      // 3. group by self
      val selfGroupTableName = "__selfGroup"
      val dupColName = details.getStringOrKey(_dup)
      val accuDupColName = details.getStringOrKey(_accu_dup)
      val selfGroupSql = {
        s"""
           |SELECT ${distAliasesClause}, (COUNT(*) - 1) AS `${dupColName}`,
           |TRUE AS `${ConstantColumns.distinct}`
           |FROM `${sourceAliasTableName}` GROUP BY ${distAliasesClause}
          """.stripMargin
      }
      val selfGroupTransStep =
        SparkSqlTransformStep(selfGroupTableName, selfGroupSql, emptyMap, true)

      val transSteps1 = sourceAliasTransStep :: totalTransStep :: selfGroupTransStep :: Nil
      val writeSteps1 = totalMetricWriteStep :: Nil

      val ((transSteps2, writeSteps2), dupCountTableName) = procType match {
        case StreamingProcessType if (withOlderTable) =>
          // 4.0 update old data
          val targetDsUpdateWriteStep = DataSourceUpdateWriteStep(targetName, targetName)

          // 4. older alias
          val olderAliasTableName = "__older"
          val olderAliasSql = {
            s"SELECT ${selClause} FROM `${targetName}` WHERE `${ConstantColumns.tmst}` <= ${beginTmst}"
          }
          val olderAliasTransStep = SparkSqlTransformStep(olderAliasTableName, olderAliasSql, emptyMap)

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
               |SELECT ${selfSelClause}, (${olderIsNull}) AS `${ConstantColumns.distinct}`
               |FROM `${olderAliasTableName}` RIGHT JOIN `${selfGroupTableName}`
               |ON ${onClause}
            """.stripMargin
          }
          val joinedTransStep = SparkSqlTransformStep(joinedTableName, joinedSql, emptyMap)

          // 6. group by joined data
          val groupTableName = "__group"
          val moreDupColName = "_more_dup"
          val groupSql = {
            s"""
               |SELECT ${distAliasesClause}, `${dupColName}`, `${ConstantColumns.distinct}`,
               |COUNT(*) AS `${moreDupColName}`
               |FROM `${joinedTableName}`
               |GROUP BY ${distAliasesClause}, `${dupColName}`, `${ConstantColumns.distinct}`
             """.stripMargin
          }
          val groupTransStep = SparkSqlTransformStep(groupTableName, groupSql, emptyMap)

          // 7. final duplicate count
          val finalDupCountTableName = "__finalDupCount"
          /**
            * dupColName:      the duplicate count of duplicated items only occurs in new data,
            *                  which means the distinct one in new data is also duplicate
            * accuDupColName:  the count of duplicated items accumulated in new data and old data,
            *                  which means the accumulated distinct count in all data
            * e.g.:  new data [A, A, B, B, C, D], old data [A, A, B, C]
            *        selfGroupTable will be (A, 1, F), (B, 1, F), (C, 0, T), (D, 0, T)
            *        joinedTable will be (A, 1, F), (A, 1, F), (B, 1, F), (C, 0, F), (D, 0, T)
            *        groupTable will be (A, 1, F, 2), (B, 1, F, 1), (C, 0, F, 1), (D, 0, T, 1)
            *        finalDupCountTable will be (A, F, 2, 3), (B, F, 2, 2), (C, F, 1, 1), (D, T, 0, 0)
            *        The distinct result of new data only should be: (A, 2), (B, 2), (C, 1), (D, 0),
            *        which means in new data [A, A, B, B, C, D], [A, A, B, B, C] are all duplicated, only [D] is distinct
            */
          val finalDupCountSql = {
            s"""
               |SELECT ${distAliasesClause}, `${ConstantColumns.distinct}`,
               |CASE WHEN `${ConstantColumns.distinct}` THEN `${dupColName}`
               |ELSE (`${dupColName}` + 1) END AS `${dupColName}`,
               |CASE WHEN `${ConstantColumns.distinct}` THEN `${dupColName}`
               |ELSE (`${dupColName}` + `${moreDupColName}`) END AS `${accuDupColName}`
               |FROM `${groupTableName}`
             """.stripMargin
          }
          val finalDupCountTransStep =
            SparkSqlTransformStep(finalDupCountTableName, finalDupCountSql, emptyMap, true)

          ((olderAliasTransStep :: joinedTransStep
            :: groupTransStep :: finalDupCountTransStep :: Nil,
            targetDsUpdateWriteStep :: Nil), finalDupCountTableName)
        case _ =>
          ((Nil, Nil), selfGroupTableName)
      }

      // 8. distinct metric
      val distTableName = "__distMetric"
      val distColName = details.getStringOrKey(_distinct)
      val distSql = {
        s"""
           |SELECT COUNT(*) AS `${distColName}`
           |FROM `${dupCountTableName}` WHERE `${ConstantColumns.distinct}`
         """.stripMargin
      }
      val distTransStep = SparkSqlTransformStep(distTableName, distSql, emptyMap)
      val distMetricWriteStep = {
        MetricWriteStep(distColName, distTableName, EntriesFlattenType, writeTimestampOpt)
      }

      val transSteps3 = distTransStep :: Nil
      val writeSteps3 = distMetricWriteStep :: Nil

      val duplicationArrayName = details.getString(_duplicationArray, "")
      val (transSteps4, writeSteps4) = if (duplicationArrayName.nonEmpty) {
        val recordEnable = details.getBoolean(_recordEnable, false)
        if (groupAliases.size > 0) {
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
               |`${dupCountTableName}`.`${ConstantColumns.distinct}` AS `${ConstantColumns.distinct}`
               |FROM `${sourceAliasTableName}` LEFT JOIN `${dupCountTableName}`
               |ON ${onClause}
               """.stripMargin
          }
          val informedTransStep = SparkSqlTransformStep(informedTableName, informedSql, emptyMap)

          // 10. add row number
          val rnTableName = "__rowNumber"
          val rnDistClause = distAliasesClause
          val rnSortClause = s"SORT BY `${ConstantColumns.distinct}`"
          val rnSql = {
            s"""
               |SELECT *,
               |ROW_NUMBER() OVER (DISTRIBUTE BY ${rnDistClause} ${rnSortClause}) `${ConstantColumns.rowNumber}`
               |FROM `${informedTableName}`
               """.stripMargin
          }
          val rnTransStep = SparkSqlTransformStep(rnTableName, rnSql, emptyMap)

          // 11. recognize duplicate items
          val dupItemsTableName = "__dupItems"
          val dupItemsSql = {
            s"""
               |SELECT ${allAliasesClause}, `${dupColName}` FROM `${rnTableName}`
               |WHERE NOT `${ConstantColumns.distinct}` OR `${ConstantColumns.rowNumber}` > 1
               """.stripMargin
          }
          val dupItemsTransStep = SparkSqlTransformStep(dupItemsTableName, dupItemsSql, emptyMap)
          val dupItemsWriteStep = {
            val rwName = ruleParam.getOutputOpt(RecordOutputType).flatMap(_.getNameOpt).getOrElse(dupItemsTableName)
            RecordWriteStep(rwName, dupItemsTableName, None, writeTimestampOpt)
          }

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
          val groupDupMetricTransStep =
            SparkSqlTransformStep(groupDupMetricTableName, groupDupMetricSql, emptyMap)
          val groupDupMetricWriteStep = {
            MetricWriteStep(duplicationArrayName,
              groupDupMetricTableName,
              ArrayFlattenType,
              writeTimestampOpt)
          }

          val msteps = {
            informedTransStep :: rnTransStep :: dupItemsTransStep :: groupDupMetricTransStep :: Nil
          }
          val wsteps = if (recordEnable) {
            dupItemsWriteStep :: groupDupMetricWriteStep :: Nil
          } else {
            groupDupMetricWriteStep :: Nil
          }

          (msteps, wsteps)

        } else {
          // no group by requirement
          // 9. duplicate record
          val dupRecordTableName = "__dupRecords"
          val dupRecordSelClause = procType match {
            case StreamingProcessType if (withOlderTable) =>
              s"${distAliasesClause}, `${dupColName}`, `${accuDupColName}`"

            case _ => s"${distAliasesClause}, `${dupColName}`"
          }
          val dupRecordSql = {
            s"""
               |SELECT ${dupRecordSelClause}
               |FROM `${dupCountTableName}` WHERE `${dupColName}` > 0
              """.stripMargin
          }
          val dupRecordTransStep =
            SparkSqlTransformStep(dupRecordTableName, dupRecordSql, emptyMap, true)

          val dupRecordWriteStep = {
            val rwName =
              ruleParam.getOutputOpt(RecordOutputType).flatMap(_.getNameOpt)
                .getOrElse(dupRecordTableName)

            RecordWriteStep(rwName, dupRecordTableName, None, writeTimestampOpt)
          }

          // 10. duplicate metric
          val dupMetricTableName = "__dupMetric"
          val numColName = details.getStringOrKey(_num)
          val dupMetricSql = {
            s"""
               |SELECT `${dupColName}`, COUNT(*) AS `${numColName}`
               |FROM `${dupRecordTableName}` GROUP BY `${dupColName}`
              """.stripMargin
          }
          val dupMetricTransStep = SparkSqlTransformStep(dupMetricTableName, dupMetricSql, emptyMap)
          val dupMetricWriteStep = {
            MetricWriteStep(
              duplicationArrayName,
              dupMetricTableName,
              ArrayFlattenType,
              writeTimestampOpt
            )
          }

          val msteps = {
            dupRecordTransStep :: dupMetricTransStep :: Nil
          }
          val wsteps = if (recordEnable) {
            dupRecordWriteStep :: dupMetricWriteStep :: Nil
          } else {
            dupMetricWriteStep :: Nil
          }

          (msteps, wsteps)
        }
      } else (Nil, Nil)

      // full steps
      transSteps1 ++ transSteps2 ++ transSteps3 ++ transSteps4 ++
        writeSteps1 ++ writeSteps2 ++ writeSteps3 ++ writeSteps4

    }
  }

}
