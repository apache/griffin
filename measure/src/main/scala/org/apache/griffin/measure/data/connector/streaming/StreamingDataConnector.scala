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
package org.apache.griffin.measure.data.connector.streaming

import org.apache.griffin.measure.data.connector._
import org.apache.griffin.measure.data.source.DataSourceCache
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.InputDStream

import scala.util.Try


trait StreamingDataConnector extends DataConnector {

  type K
  type V

  protected def stream(): Try[InputDStream[(K, V)]]

  def transform(rdd: RDD[(K, V)]): Option[DataFrame]

  def data(ms: Long): Option[DataFrame] = None

  var dataSourceCacheOpt: Option[DataSourceCache] = None

}
