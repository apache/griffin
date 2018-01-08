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
package org.apache.griffin.measure.config.params.user

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.config.params.Param

@JsonInclude(Include.NON_NULL)
case class UserParam( @JsonProperty("name") name: String,
                      @JsonProperty("timestamp") timestamp: Long,
                      @JsonProperty("process.type") procType: String,
                      @JsonProperty("data.sources") dataSourceParams: List[DataSourceParam],
                      @JsonProperty("evaluate.rule") evaluateRuleParam: EvaluateRuleParam
                    ) extends Param {

  private val validDs = {
    val (validDsParams, _) = dataSourceParams.foldLeft((Nil: Seq[DataSourceParam], Set[String]())) { (ret, dsParam) =>
      val (seq, names) = ret
      if (dsParam.hasName && !names.contains(dsParam.name)) {
        (seq :+ dsParam, names + dsParam.name)
      } else ret
    }
    validDsParams
  }
  private val baselineDsOpt = {
    val baselines = validDs.filter(_.isBaseLine)
    if (baselines.size > 0) baselines.headOption
    else validDs.headOption
  }

  val baselineDsName = baselineDsOpt match {
    case Some(ds) => ds.name
    case _ => ""
  }
  val dataSources = {
    validDs.map { ds =>
      if (ds.name != baselineDsName && ds.isBaseLine) {
        ds.falseBaselineClone
      } else ds
    }
  }

  override def validate(): Boolean = {
    dataSources.size > 0
  }

}
