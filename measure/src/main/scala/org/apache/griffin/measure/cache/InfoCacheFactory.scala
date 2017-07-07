package org.apache.griffin.measure.cache

import org.apache.griffin.measure.config.params.env.InfoCacheParam

import scala.util.{Success, Try}

case class InfoCacheFactory(infoCacheParams: Iterable[InfoCacheParam], metricName: String) extends Serializable {

  val ZK_REGEX = """^(?i)zk|zookeeper$""".r

  def getInfoCache(infoCacheParam: InfoCacheParam): Option[InfoCache] = {
    val config = infoCacheParam.config
    val infoCacheTry = infoCacheParam.persistType match {
      case ZK_REGEX() => Try(ZKInfoCache(config, metricName))
      case _ => throw new Exception("not supported info cache type")
    }
    infoCacheTry match {
      case Success(infoCache) => Some(infoCache)
      case _ => None
    }
  }

}