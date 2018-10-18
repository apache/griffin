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

import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.datasource.TimestampStorage

/**
  * Batch Data Connector for MS SQL Server (JDBC)
  *
  * @param sparkSession     Spark session
  * @param dcParam          Data Connector Parameters
  * @param timestampStorage TimestampStorage
  */
case class SqlServerBatchDataConnector(@transient sparkSession: SparkSession,
                                       dcParam: DataConnectorParam,
                                       timestampStorage: TimestampStorage
                                      ) extends JdbcBatchDataConnector {
  def getConnectionProperties(): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    connectionProperties
  }

  override def jdbcUrl(): String = {
    s"jdbc:sqlserver://${serverAndPort};database=${database}"
  }
}