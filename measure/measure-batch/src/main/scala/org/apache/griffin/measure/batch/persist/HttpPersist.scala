package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.result._
import org.apache.griffin.measure.batch.utils.{HttpUtil, JsonUtil}
import org.apache.spark.rdd.RDD

import scala.util.Try

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
    try {
      result match {
        case ar: AccuracyResult => {
          val dataMap = Map[String, Any](("name" -> metricName), ("tmst" -> timeStamp), ("total" -> ar.getTotal), ("matched" -> ar.getMatch))
          val data = JsonUtil.toJson(dataMap)

          // post
          val params = Map[String, Object]()
//          val header = Map[String, Object](("content-type" -> "application/json"))
          val header = Map[String, Object]()
          val status = HttpUtil.httpRequest(api, method, params, header, data)
          info(s"${method} to ${api} response status: ${status}")
        }
        case _ => {
          info(s"result: ${result}")
        }
      }
    } catch {
      case e => error(e.getMessage)
    }
  }

  def missRecords(records: RDD[String]): Unit = {}

  def log(rt: Long, msg: String): Unit = {}

}
