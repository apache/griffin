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

package org.apache.griffin.measure.config.params.env

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.griffin.measure.config.params.Param

/**
  * Created by xiaoqiu.duan on 2017/10/23.
  */
@JsonInclude(Include.NON_NULL)
case class EmailParam(@JsonProperty("host") host: String,
                       @JsonProperty("mail") mail: String,
                       @JsonProperty("user") usr: String,
                       @JsonProperty("password") pwd: String
                     ) extends Param {

}
