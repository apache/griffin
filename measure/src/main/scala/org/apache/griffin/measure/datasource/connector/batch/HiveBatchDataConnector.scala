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
package org.apache.griffin.measure.datasource.connector.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * batch data connector for hive table
  */
case class HiveBatchDataConnector(@transient sparkSession: SparkSession,
                                  dcParam: DataConnectorParam,
                                  timestampStorage: TimestampStorage
                                 ) extends BatchDataConnector {

  val config = dcParam.getConfig

  val Database = "database"
  val TableName = "table.name"
  val Where = "where"

  val database = config.getString(Database, "default")
  val tableName = config.getString(TableName, "")
  val whereString = config.getString(Where, "")

  val concreteTableName = s"${database}.${tableName}"
  val wheres = whereString.split(",").map(_.trim).filter(_.nonEmpty)

  def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = try {
      val dtSql = dataSql
      info(dtSql)
      val df = sparkSession.sql(dtSql)
      val dfOpt = Some(df)
      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    } catch {
      case e: Throwable =>
        error(s"load hive table ${concreteTableName} fails: ${e.getMessage}", e)
        None
    }
    val tmsts = readTmst(ms)
    (dfOpt, TimeRange(ms, tmsts))
  }


  private def tableExistsSql(): String = {
//    s"SHOW TABLES LIKE '${concreteTableName}'"    // this is hive sql, but not work for spark sql
    s"tableName LIKE '${tableName}'"
  }

  private def metaDataSql(): String = {
    s"DESCRIBE ${concreteTableName}"
  }

  private def dataSql(): String = {
    val tableClause = s"SELECT * FROM ${concreteTableName}"
    if (wheres.length > 0) {
      val clauses = wheres.map { w =>
        s"${tableClause} WHERE ${w}"
      }
      clauses.mkString(" UNION ALL ")
    } else tableClause
  }

}
