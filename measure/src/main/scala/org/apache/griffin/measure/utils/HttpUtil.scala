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

import scalaj.http._

object HttpUtil {

  val GET_REGEX = """^(?i)get$""".r
  val POST_REGEX = """^(?i)post$""".r
  val PUT_REGEX = """^(?i)put$""".r
  val DELETE_REGEX = """^(?i)delete$""".r

  def postData(url: String, params: Map[String, Object], headers: Map[String, Object], data: String): Boolean = {
    val response = Http(url).params(convertObjMap2StrMap(params)).headers(convertObjMap2StrMap(headers)).postData(data).asString
    response.isSuccess
  }

  def httpRequest(url: String, method: String, params: Map[String, Object], headers: Map[String, Object], data: String): Boolean = {
    val httpReq = Http(url).params(convertObjMap2StrMap(params)).headers(convertObjMap2StrMap(headers))
    method match {
      case POST_REGEX() => {
        val res = httpReq.postData(data).asString
        res.isSuccess
      }
      case PUT_REGEX() => {
        val res = httpReq.put(data).asString
        res.isSuccess
      }
      case _ => false
    }
  }

  private def convertObjMap2StrMap(map: Map[String, Object]): Map[String, String] = {
    map.map(pair => pair._1 -> pair._2.toString)
  }

}
