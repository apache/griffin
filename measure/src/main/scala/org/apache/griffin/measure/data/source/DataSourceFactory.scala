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
package org.apache.griffin.measure.data.source

import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.data.connector.batch.BatchDataConnector
import org.apache.griffin.measure.data.connector.streaming.StreamingDataConnector
import org.apache.griffin.measure.data.connector.{DataConnector, DataConnectorFactory}
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.process.engine.{DqEngine, DqEngines}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext

import scala.util.{Success, Try}

object DataSourceFactory extends Loggable {

  val HiveRegex = """^(?i)hive$""".r
  val TextRegex = """^(?i)text$""".r
  val AvroRegex = """^(?i)avro$""".r

  def genDataSources(sqlContext: SQLContext, ssc: StreamingContext, dqEngines: DqEngines,
                     dataSourceParams: Seq[DataSourceParam], metricName: String): Seq[DataSource] = {
    dataSourceParams.zipWithIndex.flatMap { pair =>
      val (param, index) = pair
      genDataSource(sqlContext, ssc, dqEngines, param, metricName, index)
    }
  }

  private def genDataSource(sqlContext: SQLContext, ssc: StreamingContext,
                            dqEngines: DqEngines,
                            dataSourceParam: DataSourceParam,
                            metricName: String, index: Int
                           ): Option[DataSource] = {
    val name = dataSourceParam.name
    val connectorParams = dataSourceParam.connectors
    val cacheParam = dataSourceParam.cache
    val dataConnectors = connectorParams.flatMap { connectorParam =>
      DataConnectorFactory.getDataConnector(sqlContext, ssc, dqEngines, connectorParam) match {
        case Success(connector) => Some(connector)
        case _ => None
      }
    }
    val dataSourceCacheOpt = genDataSourceCache(sqlContext, cacheParam, metricName, index)

    Some(DataSource(sqlContext, name, dataConnectors, dataSourceCacheOpt))
  }

  private def genDataSourceCache(sqlContext: SQLContext, param: Map[String, Any],
                                 metricName: String, index: Int
                                ) = {
    if (param != null) {
      try {
        Some(DataSourceCache(sqlContext, param, metricName, index))
      } catch {
        case e: Throwable => {
          error(s"generate data source cache fails")
          None
        }
      }
    } else None
  }

}
