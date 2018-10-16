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

import java.util.Properties

import org.apache.spark.sql.DataFrame

import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * Batch data connector for JDBC database
  */
trait JdbcBatchDataConnector extends BatchDataConnector {

  val config = dcParam.getConfig

  val Url = "url"
  val Database = "database"
  val Table = "table"
  val Where = "where"
  val User = "user"
  val Password = "password"

  val serverAndPort = config.getString(Url, "")
  val database = config.getString(Database, "")
  val table = config.getString(Table, "")
  val whereString = config.getString(Where, "")
  val user = config.getString(User, "")
  val password = config.getString(Password, "")

  val wheres = whereString.split(",").map(_.trim).filter(_.nonEmpty)

  def getConnectionProperties(): Properties

  def jdbcUrl(): String

  def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = try {
      assert(!serverAndPort.isEmpty && !user.isEmpty && !password.isEmpty && !table.isEmpty,
        "Missing Datasource config parameters")
      val dbUrl = jdbcUrl()
      info(dbUrl)
      val dtSql = dataSql
      info(dtSql)
      val df = sparkSession.read.jdbc(dbUrl, dtSql, getConnectionProperties)
      val dfOpt = Some(df)
      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    }
    catch {
      case e: Throwable =>
        error(s"load JDBC table ${table} fails: ${e.getMessage}")
        None
    }
    val tmsts = readTmst(ms)
    (dfOpt, TimeRange(ms, tmsts))
  }

  private def dataSql(): String = {
    val tableName = if (table.contains(" ")) List("(", table, ")").mkString else table

    val tableClause = s"SELECT * FROM ${tableName}"
    if (wheres.length > 0) {
      val clauses = wheres.map { w =>
        s"${tableClause} WHERE ${w}"
      }
      s"(${clauses.mkString(" UNION ALL ")}) as t"
    } else s"(${tableClause}) as t"
  }
}
