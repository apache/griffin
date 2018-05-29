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
package org.apache.griffin.measure.configuration.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

/**
  * full set of griffin configuration
  * @param envParam   environment configuration (must)
  * @param dqParam    dq measurement configuration (must)
  */
@JsonInclude(Include.NON_NULL)
case class AllParam( @JsonProperty("env") envParam: EnvParam,
                     @JsonProperty("dq") dqParam: DQParam
                   ) extends Param {
  override def validate(): Boolean = {
    envParam != null && dqParam != null
  }
}
