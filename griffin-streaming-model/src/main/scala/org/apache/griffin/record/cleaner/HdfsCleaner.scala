//package org.apache.griffin.record.cleaner
//
//import java.io.{BufferedReader, InputStreamReader}
//import java.util.{Date, Timer, TimerTask}
//
//import com.ebay.griffin.config.params.{AppParam, DataParam}
//
//
//class HdfsCleaner(appParam: AppParam, dataParam: DataParam) {
//
//  val expireDefSec: Long = 604800
//  val periodDefSec: Long = 7200
//
//  val timer = new Timer
//
//  def start(): Unit = {
//    val path = appParam.getParam(appParam.RecorderHdfsDir).toString
//    val metricName = dataParam.getParam(dataParam.MetricName).toString
//    val dirPath = getFilePath(path, metricName)
//
//    val expireSec = if (appParam.containsParam(appParam.RecorderCleanExpireSeconds)) {
//      appParam.getParam(appParam.RecorderCleanExpireSeconds).toString.toLong
//    } else expireDefSec
//    val periodSec = if (appParam.containsParam(appParam.RecorderCleanPeriodSeconds)) {
//      appParam.getParam(appParam.RecorderCleanPeriodSeconds).toString.toLong
//    } else periodDefSec
//
//    val expireMs = expireSec * 1000
//    val periodMs = periodSec * 1000
//
//    timer.scheduleAtFixedRate(new TimerTask() {
//      override def run(): Unit = {
//        val now = new Date().getTime
//        clean(dirPath, now - expireMs)
//      }
//    }, periodMs, periodMs)
//  }
//
//  def end(): Unit = {
//    timer.cancel
//  }
//
//  private def clean(dirPath: String, expireLineMs: Long): Unit = {
//    val process = Runtime.getRuntime.exec("hadoop fs -ls " + dirPath)
//    val result = process.waitFor()
//    if (result == 0) {
//      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
//      var line = reader.readLine
//      while (line != null) {
//        val index = line.indexOf("/")
//        if (index >= 0) {
//          val dir = line.substring(index)
//          val tail = dir.split("/").last
//          val t = try {
//            tail.toLong
//          } catch {
//            case _ => Long.MaxValue
//          }
//
//          if (t < expireLineMs) {  // expire
//            val remove = Runtime.getRuntime().exec("hadoop fs -rm -R " + dir)
//            val removeResult = remove.waitFor()
//            if (removeResult == 0) {
//              println(s"===== hadoop fs -rm -R ${dir} =====")
//            }
//          }
//        }
//        line = reader.readLine
//      }
//    }
//  }
//
//  protected def getFilePath(parentPath: String, fileName: String): String = {
//    val sep = "/"
//    if (parentPath.endsWith(sep)) parentPath + fileName else parentPath + sep + fileName
//  }
//
//}
