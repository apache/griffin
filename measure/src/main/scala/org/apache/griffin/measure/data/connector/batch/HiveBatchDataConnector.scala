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
package org.apache.griffin.measure.data.connector.batch

import org.apache.griffin.measure.config.params.user.DataConnectorParam
import org.apache.griffin.measure.data.connector._
import org.apache.griffin.measure.process.engine.DqEngines
import org.apache.griffin.measure.result._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Success, Try}
import org.apache.griffin.measure.utils.ParamUtil._

// data connector for hive
case class HiveBatchDataConnector(sqlContext: SQLContext, dqEngines: DqEngines, dcParam: DataConnectorParam
                                  ) extends BatchDataConnector {

  val config = dcParam.config

  if (!sqlContext.isInstanceOf[HiveContext]) {
    throw new Exception("hive context not prepared!")
  }

  val Database = "database"
  val TableName = "table.name"
  val Partitions = "partitions"

  val database = config.getString(Database, "default")
  val tableName = config.getString(TableName, "")
  val partitionsString = config.getString(Partitions, "")

  val concreteTableName = s"${database}.${tableName}"
  val partitions = partitionsString.split(";").map(s => s.split(",").map(_.trim))

  def data(ms: Long): Option[DataFrame] = {
    try {
      val df = sqlContext.sql(dataSql)
      val dfOpt = Some(df)
      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    } catch {
      case e: Throwable => {
        error(s"load hive table ${concreteTableName} fails")
        None
      }
    }
  }

//  def available(): Boolean = {
//    (!tableName.isEmpty) && {
//      Try {
//        if (dbPrefix) {
//          sqlContext.tables(database).filter(tableExistsSql).collect.size
//        } else {
//          sqlContext.tables().filter(tableExistsSql).collect.size
//        }
//      } match {
//        case Success(s) => s > 0
//        case _ => false
//      }
//    }
//  }

//  def init(): Unit = {}

//  def metaData(): Try[Iterable[(String, String)]] = {
//    Try {
//      val originRows = sqlContext.sql(metaDataSql).map(r => (r.getString(0), r.getString(1))).collect
//      val partitionPos: Int = originRows.indexWhere(pair => pair._1.startsWith("# "))
//      if (partitionPos < 0) originRows
//      else originRows.take(partitionPos)
//    }
//  }

//  def data(): Try[RDD[(Product, (Map[String, Any], Map[String, Any]))]] = {
//    Try {
//      sqlContext.sql(dataSql).flatMap { row =>
//        // generate cache data
//        val cacheExprValueMaps = ExprValueUtil.genExprValueMaps(Some(row), ruleExprs.cacheExprs, constFinalExprValueMap)
//        val finalExprValueMaps = ExprValueUtil.updateExprValueMaps(ruleExprs.finalCacheExprs, cacheExprValueMaps)
//
//        // data info
//        val dataInfoMap: Map[String, Any] = DataInfo.cacheInfoList.map { info =>
//          try {
//            (info.key -> row.getAs[info.T](info.key))
//          } catch {
//            case e: Throwable => info.defWrap
//          }
//        }.toMap
//
//        finalExprValueMaps.flatMap { finalExprValueMap =>
//          val groupbyData: Seq[AnyRef] = ruleExprs.groupbyExprs.flatMap { expr =>
//            expr.calculate(finalExprValueMap) match {
//              case Some(v) => Some(v.asInstanceOf[AnyRef])
//              case _ => None
//            }
//          }
//          val key = toTuple(groupbyData)
//
//          Some((key, (finalExprValueMap, dataInfoMap)))
//        }
//      }
//    }
//  }

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

//  private def toTuple[A <: AnyRef](as: Seq[A]): Product = {
//    if (as.size > 0) {
//      val tupleClass = Class.forName("scala.Tuple" + as.size)
//      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
//    } else None
//  }

}
