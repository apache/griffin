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

import org.scalatest.{FlatSpec, Matchers}

class HttpUtilTest extends FlatSpec with Matchers {
  val url = "http://10.148.181.248:39200/griffin/accuracy"
  val method = "post"
  val user = ""
  val password = ""
  val params = Map[String, Object]()
  val header = Map[String, Object](("Content-Type", "application/json"))
  val data = "{}"

  "postData httpReq" should "be with auth" in {
    try {
      HttpUtil.postData(url, user, password, params, header, data)
    } catch {
      case e: Throwable => println("cannot connect http client." + e)
    }
  }

  "postData httpReq" should "be without auth" in {
    try {
      HttpUtil.postData(url, null, null, params, header, data)
    } catch {
      case e: Throwable => println("cannot connect http client." + e)
    }
  }

  "httpReq" should "be with auth" in {
    try {
      HttpUtil.httpRequest(url, method, user, password, params, header, data)
    } catch {
      case e: Throwable => println("cannot connect http client." + e)
    }
  }

  "httpReq" should "be without auth" in {
    try {
      HttpUtil.httpRequest(url, method, null, null, params, header, data)
    } catch {
      case e: Throwable => println("cannot connect http client." + e)
    }
  }
}
