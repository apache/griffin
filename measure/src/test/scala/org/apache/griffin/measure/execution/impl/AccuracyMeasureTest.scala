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

import org.apache.griffin.measure.configuration.dqdefinition.MeasureParam
import org.apache.griffin.measure.execution.Measure._
import org.apache.griffin.measure.execution.impl.AccuracyMeasure._

class AccuracyMeasureTest extends MeasureTest {
  var param: MeasureParam = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    param = MeasureParam(
      "param",
      "Accuracy",
      "source",
      Map(
        Expression -> Seq(Map(SourceColStr -> "gender", TargetColStr -> "gender")),
        TargetSourceStr -> "target"))
  }

  "AccuracyMeasure" should "validate expression config" in {

    // Validations for Accuracy Expr

    // Empty
    assertThrows[AssertionError] {
      AccuracyMeasure(param.copy(config = Map.empty[String, String]))
    }

    // Incorrect Type and Empty
    assertThrows[AssertionError] {
      AccuracyMeasure(param.copy(config = Map(Expression -> StringUtils.EMPTY)))
    }

    // Null
    assertThrows[AssertionError] {
      AccuracyMeasure(param.copy(config = Map(Expression -> null)))
    }

    // Incorrect Type
    assertThrows[AssertionError] {
      AccuracyMeasure(param.copy(config = Map(Expression -> "gender")))
    }

    // Correct Type and Empty
    assertThrows[AssertionError] {
      AccuracyMeasure(param.copy(config = Map(Expression -> Seq.empty[Map[String, String]])))
    }

    // Invalid Expr
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config = Map(Expression -> Seq(Map("a" -> "b")), TargetSourceStr -> "target")))
    }

    // Invalid Expr as target.col is missing
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config =
          Map(Expression -> Seq(Map(SourceColStr -> "b")), TargetSourceStr -> "target")))
    }

    // Invalid Expr as source.col is missing
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config =
          Map(Expression -> Seq(Map(TargetColStr -> "b")), TargetSourceStr -> "target")))
    }

    // Validations for Target source

    // Empty
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config =
          Map(Expression -> Seq(Map("a" -> "b")), TargetSourceStr -> StringUtils.EMPTY)))
    }

    // Incorrect Type
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config = Map(Expression -> Seq(Map("a" -> "b")), TargetSourceStr -> 2331)))
    }

    // Null
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config = Map(Expression -> Seq(Map("a" -> "b")), TargetSourceStr -> null)))
    }

    // Invalid target
    assertThrows[AssertionError] {
      AccuracyMeasure(
        param.copy(config = Map(Expression -> Seq(Map("a" -> "b")), TargetSourceStr -> "jj")))
    }
  }

  it should "support metric writing" in {
    val measure = AccuracyMeasure(param)
    assertResult(true)(measure.supportsMetricWrite)
  }

  it should "support record writing" in {
    val measure = AccuracyMeasure(param)
    assertResult(true)(measure.supportsRecordWrite)
  }

  it should "execute defined measure expr" in {
    val measure = AccuracyMeasure(param)
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
    assertResult(metricMap(AccurateStr))("2")
    assertResult(metricMap(InAccurateStr))("3")
  }

}
