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
package org.apache.griffin.measure.batch.persist

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Try, Failure}

@RunWith(classOf[JUnitRunner])
class HdfsPersistTest extends FunSuite with Matchers with BeforeAndAfter {

  val config: Map[String, Any] = Map[String, Any](
    ("path" -> "/path/to"), ("max.persist.lines" -> 100), ("max.lines.per.file" -> 1000))
  val metricName: String = "metric"
  val timeStamp: Long = 123456789L

  val hdfsPersist = HdfsPersist(config, metricName, timeStamp)

  test ("constructor") {
    hdfsPersist.path should be ("/path/to")
    hdfsPersist.maxPersistLines should be (100)
    hdfsPersist.maxLinesPerFile should be (1000)

    hdfsPersist.StartFile should be (s"/path/to/${metricName}/${timeStamp}/_START")
  }

  test ("avaiable") {
    hdfsPersist.available should be (true)
  }
}
