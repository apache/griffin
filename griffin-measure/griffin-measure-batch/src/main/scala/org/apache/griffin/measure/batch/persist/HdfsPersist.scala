package org.apache.griffin.measure.batch.persist

import java.util.Date

import org.apache.griffin.measure.batch.result._
import org.apache.griffin.measure.batch.utils.HdfsUtil


case class HdfsPersist(path: String, metricName: String, timeStamp: Long) extends Persist {

  val separator = "/"

  val StartFile = filePath("_START")
  val FinishFile = filePath("_FINISH")
  val ResultFile = filePath("_RESULT")

  val MissRecFile = filePath("_MISSREC")      // optional

  val LogFile = filePath("_LOG")

  val maxCountOfStrings = 5000

  var _init = true
  private def isInit = {
    val i = _init
    _init = false
    i
  }

  private def persistHead: String = {
    val dt = new Date(timeStamp)
    s"================ log of ${dt} ================\n"
  }

  private def timeHead(rt: Long): String = {
    val dt = new Date(rt)
    s"--- ${dt} ---\n"
  }

  protected def getFilePath(parentPath: String, fileName: String): String = {
    if (parentPath.endsWith(separator)) parentPath + fileName else parentPath + separator + fileName
  }

  protected def filePath(file: String): String = {
    getFilePath(path, s"${metricName}/${timeStamp}/${file}")
  }

  def start(): Unit = {
    HdfsUtil.createEmptyFile(StartFile)
  }
  def finish(): Unit = {
    HdfsUtil.createEmptyFile(FinishFile)
  }

  def result(rt: Long, result: Result): Unit = {
    val resStr = result match {
      case ar: AccuracyResult => {
        s"match percentage: ${ar.matchPercentage}\ntotal count: ${ar.getTotal}\nmiss count: ${ar.getMiss}, match count: ${ar.getMatch}\n"
      }
      case _ => {
        s"result: ${result}\n"
      }
    }
    HdfsUtil.writeContent(ResultFile, timeHead(rt) + resStr)
    log(rt, resStr)

    info(resStr)
  }

  // need to avoid string too long
  def missRecords(records: Iterable[AnyRef]): Unit = {
    val groupCount = (records.size - 1) / maxCountOfStrings + 1
    val groupedRecords = records.grouped(groupCount).zipWithIndex
    groupedRecords.foreach { pair =>
      val (rcs, idx) = pair
      val recFunc = if (idx == 0) HdfsUtil.writeContent _ else HdfsUtil.appendContent _
      val str = rcs.foldLeft("")((a, b) => s"${a}${b}\n")
      recFunc(MissRecFile, str)
    }
  }

  def log(rt: Long, msg: String): Unit = {
    val logStr = (if (isInit) persistHead else "") + timeHead(rt) + s"${msg}\n"
    HdfsUtil.appendContent(LogFile, logStr)
  }

}
