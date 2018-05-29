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
package org.apache.griffin.measure.configuration.validator

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.params._

import scala.util.Try

object ParamValidator extends Loggable with Serializable {

  /**
    * validate param
    * @param param    param to be validated
    * @tparam T       type of param
    * @return         param valid or not
    */
  def validate[T <: Param](param: T): Try[Boolean] = Try {
    param.validate
  }

}
