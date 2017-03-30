package org.apache.griffin.record

import java.util.Date

import org.apache.griffin.record.result.AccuResult
import org.apache.griffin.utils.HdfsUtil

case class HdfsRecorder(recPath: String, recMetricName: String, recTime: Long) extends Recorder(recTime) {

  val startFile = "_START"
  val outFile = "_RESULT"
  val finalOutFile = "_RESULT_FINAL"
  val errorFile = "_ERROR"
  val infoFile = "_INFO"
  val finishFile = "_FINISHED"
  val missingRecFile = "_missingRec"
  val delayFile = "_delayResult"
  val nullResponseFile = "_nullResponseResult"
  val timeFile = "_time"

  val updateResultFile = "_RESULT_UPDATE"
  val updateMissingRecFile = "_missingRec_UPDATE"
  val updateTimeFile = "_time_UPDATE"

  val maxCountOfStrings = 5000

  private def timeHead(rt: Long): String = {
    val dt = new Date(rt)
    s"--- ${dt} ---\n"
  }

  def start(): Unit = {
    val startFilePath = getFilePath(recPath, s"${recMetricName}/${getTime}/${startFile}")
    HdfsUtil.createEmptyFile(startFilePath)
  }

  def finish(): Unit = {
    HdfsUtil.createEmptyFile(getFilePath(recPath, s"${recMetricName}/${getTime}/${finishFile}"))
  }

  def error(rt: Long, msg: String): Unit = {
    val outStr = timeHead(rt) + s"${msg}\n"
    HdfsUtil.appendContent(getFilePath(recPath, s"${recMetricName}/${getTime}/${errorFile}"), outStr)
  }

  def info(rt: Long, msg: String): Unit = {
    val outStr = timeHead(rt) + s"${msg}\n"
    HdfsUtil.appendContent(getFilePath(recPath, s"${recMetricName}/${getTime}/${infoFile}"), outStr)
  }

  def accuracyResult(rt: Long, res: AccuResult): Unit = {
    val matchPercentage: Double = if (res.totalCount <= 0) 0 else (1 - res.missCount.toDouble / res.totalCount) * 100

    val outStr = timeHead(rt) + s"match percentage: ${matchPercentage}\nmiss count: ${res.missCount}\ntotal count: ${res.totalCount}\n"

    // output
    HdfsUtil.appendContent(getFilePath(recPath, s"${recMetricName}/${getTime}/${outFile}"), outStr)
    HdfsUtil.writeContent(getFilePath(recPath, s"${recMetricName}/${getTime}/${finalOutFile}"), outStr)
  }

  def accuracyMissingRecords(records: Iterable[AnyRef]): Unit = {
    val filePath = getFilePath(recPath, s"${recMetricName}/${getTime}/${missingRecFile}")
    val groupCount = (records.size - 1) / maxCountOfStrings + 1
    val groupedRecords = records.grouped(groupCount).zipWithIndex
    groupedRecords.foreach { pair =>
      val (rcs, idx) = pair
      val recFunc = if (idx == 0) HdfsUtil.writeContent _ else HdfsUtil.appendContent _
      recFunc(filePath, rcs.mkString("\n"))
    }
  }

  def recordTime(rt: Long, calcTime: Long): Unit = {
    val outStr = timeHead(rt) + s"${calcTime}\n"
    HdfsUtil.appendContent(getFilePath(recPath, s"${recMetricName}/${getTime}/${timeFile}"), outStr)
  }

//  override def recordUpdateTime(calcTime: Long): Unit = {
//    val timeFilePath = getFilePath(recPath, s"${recMetricName}/${getTime}/${updateTimeFile}")
//    HdfsUtil.appendContent(timeFilePath, s"${recTime}\n${calcTime}\n\n")
//  }

  protected def getFilePath(parentPath: String, fileName: String): String = {
    val sep = "/"
    if (parentPath.endsWith(sep)) parentPath + fileName else parentPath + sep + fileName
  }

}
