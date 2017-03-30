package org.apache.griffin.utils

import scalaj.http._

object RestfulUtil {

  def postData(url: String, params: Map[String, Object], headers: Map[String, Object], data: String): String = {
    val response = Http(url).params(convertObjMap2StrMap(params)).headers(convertObjMap2StrMap(headers)).postData(data).asString
    response.code.toString
  }

  private def convertObjMap2StrMap(map: Map[String, Object]): Map[String, String] = {
    map.map(pair => pair._1 -> pair._2.asInstanceOf[String])
  }

}
