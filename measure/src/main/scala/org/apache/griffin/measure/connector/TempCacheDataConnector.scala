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
package org.apache.griffin.measure.connector

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Try, Success}

case class TempCacheDataConnector(sqlContext: SQLContext, config: Map[String, Any]
                                 ) extends CacheDataConnector {

  val TableName = "table.name"
  val tableName = config.getOrElse(TableName, "").toString
  val tmpTableName = s"${tableName}_tmp"

  val timeStampColumn = "_tmst_"

  var tableCreated = tableExists

  val InfoPath = "info.path"
  val cacheTimeKey: String = config.getOrElse(InfoPath, "").toString

  def available(): Boolean = {
    tableName.nonEmpty
  }

  def saveData(df: DataFrame, ms: Long): Unit = {
    if (!tableCreated) {
      df.registerTempTable(tableName)
      sqlContext.cacheTable(tableName)
      tableCreated = true
    } else {
      df.registerTempTable(tmpTableName)
      sqlContext.sql(s"INSERT INTO TABLE ${tableName} SELECT * FROM ${tmpTableName}")
      sqlContext.dropTempTable(tmpTableName)
    }

    // submit ms
    submitCacheTime(ms)
  }

  def readData(): DataFrame = {
    val timeRange = readTimeRange
    val readSql = readDataSql(timeRange)
    sqlContext.sql(readSql)
  }

  private def tableExists(): Boolean = {
    Try {
      sqlContext.tables().filter(tableExistsSql).collect.size
    } match {
      case Success(s) => s > 0
      case _ => false
    }
  }

  private def tableExistsSql(): String = {
    s"tableName LIKE '${tableName}'"
  }

  private def readDataSql(timeRange: (Long, Long)): String = {
    s"SELECT * FROM ${tableName} WHERE `${timeStampColumn}` BETWEEN ${timeRange._1} AND ${timeRange._2}"
  }

}
