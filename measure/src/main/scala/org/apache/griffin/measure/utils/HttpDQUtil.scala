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

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients

import org.apache.griffin.measure.Loggable

/**
  * Utility to post Http Requests
  * While using @HttpUtil.scala, all Http request are getting converted into FsURLConnection scheme,
  * which is causing issue with Spark 2.2.1 onwards
  * Refer to <a href='https://issues.apache.org/jira/browse/SPARK-25694'>Here</a>
  */
object HttpDQUtil extends Loggable {

  val POST_REGEX = """^(?i)post$""".r

  def httpRequest(url: String,
                  method: String,
                  params: Map[String, Object],
                  headers: Map[String, Object],
                  data: String): Boolean = {
    try {

      debug(s"Url: $url")
      debug(s"Method: $method")
      debug(s"Headers: $headers")
      debug(s"Data: $data")
      val client = HttpClients.createDefault()

      method match {
        case POST_REGEX() =>
          val post = new HttpPost(url)
          headers.foreach(header => post.addHeader(header._1, header._2.toString))
          post.setEntity(new StringEntity(data))
          val resp = client.execute(post)
          debug(s"Status Code: ${resp.getStatusLine.getStatusCode}")
          resp.getStatusLine.getStatusCode == 201 || resp.getStatusLine.getStatusCode == 200
        case _ =>
          false
      }
    }
    catch {
      case e: Throwable => error(e.getMessage)
        false
    }
  }
}
