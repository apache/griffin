package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.result._
import org.apache.griffin.measure.batch.utils.{HttpUtil, JsonUtil}

case class HttpPersist(url: String, method: String, metricName: String, timeStamp: Long) extends Persist {

  def start(): Unit = {}
  def finish(): Unit = {}

  def result(rt: Long, result: Result): Unit = {
    result match {
      case ar: AccuracyResult => {
        val dataMap = Map[String, Any](("metricName" -> metricName), ("timestamp" -> timeStamp), ("value" -> ar.matchPercentage))
        val data = JsonUtil.toJson(dataMap)

        // post
        val params = Map[String, Object]()
        val header = Map[String, Object](("content-type" -> "application/json"))
        val status = HttpUtil.httpRequest(url, method, params, header, data)
        info(s"${method} to ${url} response status: ${status}")
      }
      case _ => {
        info(s"result: ${result}")
      }
    }
  }

  def missRecords(records: Iterable[AnyRef]): Unit = {}

  def log(rt: Long, msg: String): Unit = {}

}
