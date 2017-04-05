package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.config.params.env._


case class PersistFactory(persistParams: Iterable[PersistParam], metricName: String) extends Serializable {

  val HDFS_REGEX = """^(?i)hdfs$""".r
  val HTTP_REGEX = """^(?i)http$""".r

  def getPersists(timeStamp: Long): Iterable[Persist] = {
    persistParams.flatMap(param => getPersist(timeStamp, param))
  }

  private def getPersist(timeStamp: Long, persistParam: PersistParam): Option[Persist] = {
    val persistConfig = persistParam.config
    val persist = persistParam.persistType match {
      case HDFS_REGEX() => {
        persistConfig.get("path") match {
          case Some(path: String) => Some(HdfsPersist(path, metricName, timeStamp))
          case _ => None
        }
      }
      case HTTP_REGEX() => {
        persistConfig.get("api") match {
          case Some(api: String) => {
            val method = persistConfig.get("method") match {
              case Some(m: String) => m
              case _ => "GET"
            }
            Some(HttpPersist(api, method, metricName, timeStamp))
          }
          case _ => None
        }
      }
      case _ => None
    }
    persist
  }

}
