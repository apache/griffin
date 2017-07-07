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
package org.apache.griffin.measure.config.validator

import org.apache.griffin.measure.config.params._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class AllParamValidatorTest extends FlatSpec with Matchers with BeforeAndAfter with MockFactory {

  "validate" should "pass" in {
    val validator = AllParamValidator()
    val paramMock = mock[Param]
    paramMock.validate _ expects () returning (false)

    val validateTry = validator.validate(paramMock)
    validateTry.isSuccess should be (true)
    validateTry.get should be (false)
  }

}
