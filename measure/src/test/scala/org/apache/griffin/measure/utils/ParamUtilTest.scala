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
package org.apache.griffin.measure.utils

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ParamUtilTest extends FunSuite with Matchers with BeforeAndAfter {

  test ("test param util") {
    val params = Map[String, Any](
      ("a" -> "321"),
      ("b" -> 123),
      ("c" -> 3.2),
      ("d" -> (213 :: 321 :: Nil))
    )

    import ParamUtil._

    params.getString("a", "") should be ("321")
    params.getInt("b", 0) should be (123)
    params.getBoolean("c", false) should be (false)
    params.getAnyRef("d", List[Int]()) should be ((213 :: 321 :: Nil))
  }

}
