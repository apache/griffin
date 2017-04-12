package org.apache.griffin.measure.batch.persist

import java.util.Date

import org.apache.griffin.measure.batch.result._
import org.apache.griffin.measure.batch.utils.HdfsUtil
import org.apache.spark.rdd.RDD


case class HdfsPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  val Path = "path"
  val MaxPersistLines = "max.persist.lines"
  val MaxLinesPerFile = "max.lines.per.file"

  val path = config.getOrElse(Path, "").toString
  val maxPersistLines = config.getOrElse(MaxPersistLines, -1).toString.toLong
  val maxLinesPerFile = config.getOrElse(MaxLinesPerFile, 10000).toString.toLong

  val separator = "/"

  val StartFile = filePath("_START")
  val FinishFile = filePath("_FINISH")
  val ResultFile = filePath("_RESULT")

  val MissRecFile = filePath("_MISSREC")      // optional

  val LogFile = filePath("_LOG")

  var _init = true
  private def isInit = {
    val i = _init
    _init = false
    i
  }

  def available(): Boolean = {
    (path.nonEmpty) && (maxPersistLines < Int.MaxValue)
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

  protected def withSuffix(path: String, suffix: String): String = {
    s"${path}.${suffix}"
  }

  def start(msg: String): Unit = {
    HdfsUtil.writeContent(StartFile, msg)
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
  def missRecords(records: RDD[String]): Unit = {
    val recordCount = records.count
    val count = if (maxPersistLines < 0) recordCount else scala.math.min(maxPersistLines, recordCount)
    if (count > 0) {
      val groupCount = ((count - 1) / maxLinesPerFile + 1).toInt
      if (groupCount <= 1) {
        val recs = records.take(count.toInt)
        persistRecords(MissRecFile, recs)
      } else {
        val groupedRecords: RDD[(Long, Iterable[String])] =
          records.zipWithIndex.flatMap { r =>
            val gid = r._2 / maxLinesPerFile
            if (gid < groupCount) Some((gid, r._1)) else None
          }.groupByKey()
        groupedRecords.foreach { group =>
          val (gid, recs) = group
          val hdfsPath = if (gid == 0) MissRecFile else withSuffix(MissRecFile, gid.toString)
          persistRecords(hdfsPath, recs)
        }
      }
    }
  }

  private def persistRecords(hdfsPath: String, records: Iterable[String]): Unit = {
    val recStr = records.mkString("\n")
    HdfsUtil.appendContent(hdfsPath, recStr)
  }

  def log(rt: Long, msg: String): Unit = {
    val logStr = (if (isInit) persistHead else "") + timeHead(rt) + s"${msg}\n"
    HdfsUtil.appendContent(LogFile, logStr)
  }

}
