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
package org.apache.griffin.measure.rule.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MeanUdaf extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Array(StructField("item", LongType)))

  def bufferSchema = StructType(Array(
    StructField("sum", DoubleType),
    StructField("cnt", LongType)
  ))

  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.toDouble)
    buffer.update(1, 0L)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sum = buffer.getDouble(0)
    val cnt = buffer.getLong(1)
    val value = input.getLong(0)
    buffer.update(0, sum + value)
    buffer.update(1, cnt + 1)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
  }

  def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getLong(1).toDouble
  }
}