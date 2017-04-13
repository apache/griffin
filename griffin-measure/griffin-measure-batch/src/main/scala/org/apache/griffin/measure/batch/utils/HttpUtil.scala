package org.apache.griffin.measure.batch.utils

import scalaj.http._

object HttpUtil {

  val GET_REGEX = """^(?i)get$""".r
  val POST_REGEX = """^(?i)post$""".r
  val PUT_REGEX = """^(?i)put$""".r
  val DELETE_REGEX = """^(?i)delete$""".r

  def postData(url: String, params: Map[String, Object], headers: Map[String, Object], data: String): String = {
    val response = Http(url).params(convertObjMap2StrMap(params)).headers(convertObjMap2StrMap(headers)).postData(data).asString
    response.code.toString
  }

  def httpRequest(url: String, method: String, params: Map[String, Object], headers: Map[String, Object], data: String): String = {
    val httpReq = Http(url).params(convertObjMap2StrMap(params)).headers(convertObjMap2StrMap(headers))
    method match {
      case POST_REGEX() => httpReq.postData(data).asString.code.toString
      case PUT_REGEX() => httpReq.put(data).asString.code.toString
      case _ => "wrong method"
    }
  }

  private def convertObjMap2StrMap(map: Map[String, Object]): Map[String, String] = {
    map.map(pair => pair._1 -> pair._2.toString)
  }

}
