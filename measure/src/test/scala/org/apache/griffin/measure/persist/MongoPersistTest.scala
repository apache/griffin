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
package org.apache.griffin.measure.persist

import org.junit.runner.RunWith
import org.mongodb.scala.{Completed, Document}
import org.mongodb.scala.model.{Filters, UpdateOptions, Updates}
import org.mongodb.scala.result.UpdateResult
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class MongoPersistTest extends FunSuite with Matchers with BeforeAndAfter {

  val config = Map[String, Any](
    ("url" -> "mongodb://111.111.111.111"),
    ("database" -> "db"),
    ("collection" -> "cl")
  )
  val metricName: String = "metric"
  val timeStamp: Long = 123456789L

  val mongoPersist = MongoPersist(config, metricName, timeStamp)

  test("available") {
    mongoPersist.available should be (true)
  }

}
