package org.apache.griffin.record

import org.apache.griffin.utils.{RestfulUtil, JsonUtil}

import org.apache.griffin.record.result.AccuResult

//case class PostRecorder(url: String, path: String, metricName: String, t: Long) extends HdfsRecorder(path, metricName, t) {
case class PostRecorder(url: String, metricName: String, recTime: Long) extends Recorder(recTime) {

  val api = "api/v1/metrics"
  val urlPath = getUrlPath(url, api)

  def accuracyResult(rt: Long, res: AccuResult): Unit = {
    val matchPercentage: Double = if (res.totalCount <= 0) 0 else (1 - res.missCount.toDouble / res.totalCount) * 100

    def getDataStr(): String = {
      val dataMap: Map[String, Any] = Map[String, Any](("metricName" -> metricName), ("timestamp" -> getTime), ("value" -> matchPercentage))
      JsonUtil.toJson(dataMap)
    }

    // post
    val params = Map[String, Object]()
    val header = Map[String, Object](("content-type" -> "application/json"))
    val data = getDataStr
    val status = RestfulUtil.postData(urlPath, params, header, data)
    println(s"post response status: ${status}")
  }

  def accuracyMissingRecords(records: Iterable[AnyRef]): Unit = {
    ;
  }

  private def getUrlPath(urlPath: String, apiPath: String): String = {
    val sep = "/"
    if (urlPath.endsWith(sep)) urlPath + apiPath else urlPath + sep + apiPath
  }

  def start(): Unit = {}
  def finish(): Unit = {}
  def error(rt: Long, msg: String): Unit = {}
  def info(rt: Long, msg: String): Unit = {}
//  def delayResult(res: (Long, Iterable[AnyRef])): Unit = {}
//  def nullResponseResult(res: (Long, Iterable[AnyRef])): Unit = {}
  def recordTime(rt: Long, calcTime: Long): Unit = {}

}
