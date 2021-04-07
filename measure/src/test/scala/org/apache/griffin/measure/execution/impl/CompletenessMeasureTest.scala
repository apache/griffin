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

package org.apache.griffin.measure.execution.impl

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure._

class CompletenessMeasureTest extends MeasureTest {
  var param: MeasureParam = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    param = MeasureParam("param", "Completeness", "source", Map(Expression -> "name is null"))
  }

  "CompletenessMeasure" should "validate expression config" in {
    assertThrows[AssertionError] {
      CompletenessMeasure(param.copy(config = Map.empty[String, String]))
    }

    assertThrows[AssertionError] {
      CompletenessMeasure(param.copy(config = Map(Expression -> StringUtils.EMPTY)))
    }

    assertThrows[AssertionError] {
      CompletenessMeasure(param.copy(config = Map(Expression -> null)))
    }

    assertThrows[AssertionError] {
      CompletenessMeasure(param.copy(config = Map(Expression -> 22)))
    }
  }

  it should "support metric writing" in {
    val measure = CompletenessMeasure(param)
    assertResult(true)(measure.supportsMetricWrite)
  }

  it should "support record writing" in {
    val measure = CompletenessMeasure(param)
    assertResult(true)(measure.supportsRecordWrite)
  }

  it should "execute defined measure expr" in {
    val measure = CompletenessMeasure(param)
    val (recordsDf, metricsDf) = measure.execute(context, None)

    assertResult(recordDfSchema)(recordsDf.schema)
    assertResult(metricDfSchema)(metricsDf.schema)

    assertResult(source.count())(recordsDf.count())
    assertResult(1L)(metricsDf.count())

    val row = metricsDf.head()
    assertResult(param.getDataSource)(row.getAs[String](DataSource))
    assertResult(param.getName)(row.getAs[String](MeasureName))
    assertResult(param.getType.toString)(row.getAs[String](MeasureType))

    val metricMap = row.getAs[Map[String, String]](Metrics)
    assertResult(metricMap(Total))("5")
    assertResult(metricMap(measure.Complete))("4")
    assertResult(metricMap(measure.InComplete))("1")
  }

  it should "supported complex measure expr" in {
    val measure = CompletenessMeasure(
      param.copy(config = Map(Expression -> "name is null or gender is null")))
    val (recordsDf, metricsDf) = measure.execute(context, None)

    assertResult(recordDfSchema)(recordsDf.schema)
    assertResult(metricDfSchema)(metricsDf.schema)

    assertResult(source.count())(recordsDf.count())
    assertResult(1L)(metricsDf.count())

    val row = metricsDf.head()
    assertResult(param.getDataSource)(row.getAs[String](DataSource))
    assertResult(param.getName)(row.getAs[String](MeasureName))
    assertResult(param.getType.toString)(row.getAs[String](MeasureType))

    val metricMap = row.getAs[Map[String, String]](Metrics)
    assertResult(metricMap(Total))("5")
    assertResult(metricMap(measure.Complete))("3")
    assertResult(metricMap(measure.InComplete))("2")
  }

  it should "throw runtime error for invalid expr" in {
    assertThrows[AnalysisException] {
      CompletenessMeasure(param.copy(config = Map(Expression -> "xyz is null")))
        .execute(context)
    }

    assertThrows[ParseException] {
      CompletenessMeasure(param.copy(config = Map(Expression -> "select 1")))
        .execute(context)
    }
  }

}
