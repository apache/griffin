/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.griffin.measure.datasource.connector.batch

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import org.apache.griffin.measure.SparkSuiteBase
import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.datasource.TimestampStorage


class ElasticSearchGriffinDataConnectorTest extends SparkSuiteBase with Matchers with MockitoSugar {
  val tsStorage = TimestampStorage()

  val configs: Map[String, Any] = Map(
    "sql.mode" -> true,
    "host" -> "localhost",
    "port" -> "80",
    "sql" -> "select col_a, col_b, col_c from index-s limit 5")

  val dcParam = DataConnectorParam("custom", "1", "test_df", configs, Nil)

  val csvData: String =
    """col_a,col_b,col_c
      |1,test1,2.5
      |2,test2,3.5
      |3,test3,4.5
      |4,test4,5.5""".stripMargin

  it should "be able to get df from csv data" in {
    val dc = spy(ElasticSearchGriffinDataConnector(spark, dcParam.copy(config = configs), tsStorage))
    doReturn((true, csvData)).when(dc).httpPost(anyString(), anyString())
    val result = dc.data(1000L)
    val dfOpt = result._1
    assert(dfOpt.isDefined)
    val df = dfOpt.get
    val columns = df.columns
    assert(columns.contains("col_a"))
    assert(columns.contains("col_b"))
    assert(columns.contains("col_c"))
    val rows = df.collect()
    assert(rows.length == 4)
  }
}
