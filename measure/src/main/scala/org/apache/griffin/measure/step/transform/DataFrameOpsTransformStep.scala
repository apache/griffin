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
package org.apache.griffin.measure.step.transform

import org.apache.griffin.measure.context.DQContext

/**
  * data frame ops transform step
  */
case class DataFrameOpsTransformStep(name: String,
                                     inputDfName: String,
                                     rule: String,
                                     details: Map[String, Any],
                                     cache: Boolean = false
                                    ) extends TransformStep {

  def execute(context: DQContext): Boolean = {
    val sqlContext = context.sqlContext
    try {
      val df = rule match {
        case DataFrameOps._fromJson => DataFrameOps.fromJson(sqlContext, inputDfName, details)
        case DataFrameOps._accuracy =>
          DataFrameOps.accuracy(sqlContext, inputDfName, context.contextId, details)

        case DataFrameOps._clear => DataFrameOps.clear(sqlContext, inputDfName, details)
        case _ => throw new Exception(s"df opr [ ${rule} ] not supported")
      }
      if (cache) context.dataFrameCache.cacheDataFrame(name, df)
      context.runTimeTableRegister.registerTable(name, df)
      true
    } catch {
      case e: Throwable =>
        error(s"run data frame ops [ ${rule} ] error: ${e.getMessage}", e)
        false
    }
  }

}
