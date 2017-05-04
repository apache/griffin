package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.result._
import org.apache.griffin.measure.batch.utils.{HttpUtil, JsonUtil}
import org.apache.spark.rdd.RDD

case class HttpPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  val Api = "api"
  val Method = "method"

  val api = config.getOrElse(Api, "").toString
  val method = config.getOrElse(Method, "post").toString

  def available(): Boolean = {
    api.nonEmpty
  }

  def start(msg: String): Unit = {}
  def finish(): Unit = {}

  def result(rt: Long, result: Result): Unit = {
    result match {
      case ar: AccuracyResult => {
        val dataMap = Map[String, Any](("metricName" -> metricName), ("timestamp" -> timeStamp), ("value" -> ar.matchPercentage))
        val data = JsonUtil.toJson(dataMap)

        // post
        val params = Map[String, Object]()
        val header = Map[String, Object](("content-type" -> "application/json"))
        val status = HttpUtil.httpRequest(api, method, params, header, data)
        info(s"${method} to ${api} response status: ${status}")
      }
      case _ => {
        info(s"result: ${result}")
      }
    }
  }

  def missRecords(records: RDD[String]): Unit = {}

  def log(rt: Long, msg: String): Unit = {}

}
