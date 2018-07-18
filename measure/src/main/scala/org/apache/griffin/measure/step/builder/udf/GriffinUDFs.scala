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
package org.apache.griffin.measure.step.builder.udf

import org.apache.spark.sql.SQLContext

object GriffinUDFAgent {
  def register(sqlContext: SQLContext): Unit = {
    GriffinUDFs.register(sqlContext)
    GriffinUDAggFs.register(sqlContext)
  }
}

/**
  * user defined functions extension
  */
object GriffinUDFs {

  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("index_of", indexOf _)
    sqlContext.udf.register("matches", matches _)
    sqlContext.udf.register("reg_replace", regReplace _)
  }

  private def indexOf(arr: Seq[String], v: String) = {
    arr.indexOf(v)
  }

  private def matches(s: String, regex: String) = {
    s.matches(regex)
  }

  private def regReplace(s: String, regex: String, replacement: String) = {
    s.replaceAll(regex, replacement)
  }

}

/**
  * aggregation functions extension
  */
object GriffinUDAggFs {

  def register(sqlContext: SQLContext): Unit = {
  }

}