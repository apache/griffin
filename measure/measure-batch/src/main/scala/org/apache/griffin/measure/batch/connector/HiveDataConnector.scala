/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.{ExprValueUtil, RuleExprs}
import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.{Success, Try}

// data connector for hive
case class HiveDataConnector(sqlContext: SQLContext, config: Map[String, Any],
                             ruleExprs: RuleExprs, constFinalExprValueMap: Map[String, Any]
                            ) extends DataConnector {

  val Database = "database"
  val TableName = "table.name"
  val Partitions = "partitions"

  val database = config.getOrElse(Database, "").toString
  val tableName = config.getOrElse(TableName, "").toString
  val partitionsString = config.getOrElse(Partitions, "").toString

  val concreteTableName = if (dbPrefix) s"${database}.${tableName}" else tableName
  val partitions = partitionsString.split(";").map(s => s.split(",").map(_.trim))

  private def dbPrefix(): Boolean = {
    database.nonEmpty && !database.equals("default")
  }

  def available(): Boolean = {
    (!tableName.isEmpty) && {
      Try {
        if (dbPrefix) {
          sqlContext.tables(database).filter(tableExistsSql).collect.size
        } else {
          sqlContext.tables().filter(tableExistsSql).collect.size
        }
      } match {
        case Success(s) => s > 0
        case _ => false
      }
    }
  }

  def metaData(): Try[Iterable[(String, String)]] = {
    Try {
      val originRows = sqlContext.sql(metaDataSql).map(r => (r.getString(0), r.getString(1))).collect
      val partitionPos: Int = originRows.indexWhere(pair => pair._1.startsWith("# "))
      if (partitionPos < 0) originRows
      else originRows.take(partitionPos)
    }
  }

  def data(): Try[RDD[(Product, Map[String, Any])]] = {
    Try {
      sqlContext.sql(dataSql).flatMap { row =>
        // generate cache data
        val cacheExprValueMap: Map[String, Any] = ruleExprs.cacheExprs.foldLeft(constFinalExprValueMap) { (cachedMap, expr) =>
          ExprValueUtil.genExprValueMap(Some(row), expr, cachedMap)
        }
        val finalExprValueMap = ExprValueUtil.updateExprValueMap(ruleExprs.finalCacheExprs, cacheExprValueMap)

        // when clause filter data source
        val whenResult = ruleExprs.whenClauseExprOpt match {
          case Some(whenClause) => whenClause.calculate(finalExprValueMap)
          case _ => None
        }

        // get groupby data
        whenResult match {
          case Some(false) => None
          case _ => {
            val groupbyData: Seq[AnyRef] = ruleExprs.groupbyExprs.flatMap { expr =>
              expr.calculate(finalExprValueMap) match {
                case Some(v) => Some(v.asInstanceOf[AnyRef])
                case _ => None
              }
            }
            val key = toTuple(groupbyData)

            Some((key, finalExprValueMap))
          }
        }
      }
    }
  }

  private def tableExistsSql(): String = {
//    s"SHOW TABLES LIKE '${concreteTableName}'"    // this is hive sql, but not work for spark sql
    s"tableName LIKE '${tableName}'"
  }

  private def metaDataSql(): String = {
    s"DESCRIBE ${concreteTableName}"
  }

  private def dataSql(): String = {
    val clauses = partitions.map { prtn =>
      val cls = prtn.mkString(" AND ")
      if (cls.isEmpty) s"SELECT * FROM ${concreteTableName}"
      else s"SELECT * FROM ${concreteTableName} WHERE ${cls}"
    }
    clauses.mkString(" UNION ALL ")
  }

  private def toTuple[A <: AnyRef](as: Seq[A]): Product = {
    if (as.size > 0) {
      val tupleClass = Class.forName("scala.Tuple" + as.size)
      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
    } else None
  }

}
