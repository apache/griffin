package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.config.params.env._

import scala.util.{Success, Try}


case class PersistFactory(persistParams: Iterable[PersistParam], metricName: String) extends Serializable {

  val HDFS_REGEX = """^(?i)hdfs$""".r
  val HTTP_REGEX = """^(?i)http$""".r

  def getPersists(timeStamp: Long): MultiPersists = {
    MultiPersists(persistParams.flatMap(param => getPersist(timeStamp, param)))
  }

  private def getPersist(timeStamp: Long, persistParam: PersistParam): Option[Persist] = {
    val persistConfig = persistParam.config
    val persistTry = persistParam.persistType match {
      case HDFS_REGEX() => Try(HdfsPersist(persistConfig, metricName, timeStamp))
      case HTTP_REGEX() => Try(HttpPersist(persistConfig, metricName, timeStamp))
      case _ => throw new Exception("not supported persist type")
    }
    persistTry match {
      case Success(persist) if (persist.available) => Some(persist)
      case _ => None
    }
  }

}
