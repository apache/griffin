package org.apache.griffin.record

import org.apache.griffin.config.params.RecorderParam


case class RecorderFactory(recorderParam: RecorderParam) extends Serializable {

  val HDFS_REGEX = """^(?i)hdfs$""".r
  val POST_REGEX = """^(?i)post$""".r

  val metricName = recorderParam.metricName
  val configMap = recorderParam.config

  val HDFS_DIR = "hdfs.dir"
  val POST_URL = "post.url"
  val POST_METRIC_NAME = "post.metric.name"

  def getRecorders(time: Long): Iterable[Recorder] = {
    val recorderTypes = recorderParam.types
    recorderTypes.map(tp => getRecorder(time, tp))
  }

  private def getRecorder(time: Long, recorderType: String): Recorder = {
    val recorder = recorderType match {
      case HDFS_REGEX() => {
        configMap.get(HDFS_DIR) match {
          case Some(path) => HdfsRecorder(path, metricName, time)
          case _ => NullRecorder()
        }
      }
      case POST_REGEX() => {
        configMap.get(POST_URL) match {
          case Some(url) => {
            val mtrName = configMap.get(POST_METRIC_NAME) match {
              case Some(s) => s
              case _ => metricName
            }
            PostRecorder(url, mtrName, time)
          }
          case _ => NullRecorder()
        }
      }
      case _ => NullRecorder()
    }
    recorder
  }

}
