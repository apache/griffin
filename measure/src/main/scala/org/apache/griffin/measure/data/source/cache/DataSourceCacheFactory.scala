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
package org.apache.griffin.measure.data.source.cache

import org.apache.griffin.measure.log.Loggable
import org.apache.spark.sql.SQLContext
import org.apache.griffin.measure.utils.ParamUtil._

object DataSourceCacheFactory extends Loggable {

  private object DataSourceCacheType {
    val parquet = "^(?i)parq(uet)?$".r
    val json = "^(?i)json$".r
    val orc = "^(?i)orc$".r
  }
  import DataSourceCacheType._

  val _type = "type"

  def genDataSourceCache(sqlContext: SQLContext, param: Map[String, Any],
                                 name: String, index: Int
                                ) = {
    if (param != null) {
      try {
        val tp = param.getString(_type, "")
        val dsCache = tp match {
          case parquet() => ParquetDataSourceCache(sqlContext, param, name, index)
          case json() => JsonDataSourceCache(sqlContext, param, name, index)
          case orc() => OrcDataSourceCache(sqlContext, param, name, index)
          case _ => ParquetDataSourceCache(sqlContext, param, name, index)
        }
        Some(dsCache)
      } catch {
        case e: Throwable => {
          error(s"generate data source cache fails")
          None
        }
      }
    } else None
  }

}
