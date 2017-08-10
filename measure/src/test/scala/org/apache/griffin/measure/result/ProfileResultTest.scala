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
package org.apache.griffin.measure.result

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ProfileResultTest extends FunSuite with BeforeAndAfter with Matchers {

  test ("update") {
    val result = ProfileResult(10, 100)
    val delta = ProfileResult(30, 90)
    result.update(delta) should be (ProfileResult(40, 100))
  }

  test ("eventual") {
    val result1 = ProfileResult(10, 100)
    result1.eventual should be (false)

    val result2 = ProfileResult(100, 100)
    result2.eventual should be (true)
  }

  test ("differsFrom") {
    val result = ProfileResult(10, 100)
    result.differsFrom(ProfileResult(11, 100)) should be (true)
    result.differsFrom(ProfileResult(10, 110)) should be (true)
    result.differsFrom(ProfileResult(10, 100)) should be (false)
  }

  test ("matchPercentage") {
    val result1 = ProfileResult(90, 100)
    result1.matchPercentage should be (90.0)

    val result2 = ProfileResult(10, 0)
    result2.matchPercentage should be (0.0)
  }

}
