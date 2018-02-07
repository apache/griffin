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
package org.apache.griffin.measure.rule.udf


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//import org.scalatest.FlatSpec
import org.scalatest.PrivateMethodTester
//import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class GriffinUdfsTest extends FunSuite with Matchers with BeforeAndAfter with PrivateMethodTester {

  test ("test indexOf") {
    val inv = new Invocation[Int]('indexOf, "a" :: "b" :: "c" :: Nil, "b")
    GriffinUdfs.invokePrivate(inv) should be (1)
  }

  test ("test matches") {
    val inv = new Invocation[Boolean]('matches, "s123", "^s\\d+$")
    GriffinUdfs.invokePrivate(inv) should be (true)
  }

  test ("test regexSubstr") {
    val str = "https://www.abc.com/test/dp/AAA/ref=sr_1_1/123-456?id=123"
    val regexStr = """^([^/]+://[^/]+)(?:/[^/]+)?(/dp/[^/]+)(?:/.*)?$"""
    val replacement = "$1$2"
    val inv = new Invocation[String]('regexSubstr, str, regexStr, replacement)
    GriffinUdfs.invokePrivate(inv) should be ("https://www.abc.com/dp/AAA")
  }

}
