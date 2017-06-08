/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.config.params.env._

import scala.util.{Success, Try}


case class PersistFactory(persistParams: Iterable[PersistParam], metricName: String) extends Serializable {

  val HDFS_REGEX = """^(?i)hdfs$""".r
  val HTTP_REGEX = """^(?i)http$""".r
  val LOG_REGEX = """^(?i)log$""".r

  def getPersists(timeStamp: Long): MultiPersists = {
    MultiPersists(persistParams.flatMap(param => getPersist(timeStamp, param)))
  }

  // get the persists configured
  private def getPersist(timeStamp: Long, persistParam: PersistParam): Option[Persist] = {
    val persistConfig = persistParam.config
    val persistTry = persistParam.persistType match {
      case HDFS_REGEX() => Try(HdfsPersist(persistConfig, metricName, timeStamp))
      case HTTP_REGEX() => Try(HttpPersist(persistConfig, metricName, timeStamp))
      case LOG_REGEX() => Try(LoggerPersist(persistConfig, metricName, timeStamp))
      case _ => throw new Exception("not supported persist type")
    }
    persistTry match {
      case Success(persist) if (persist.available) => Some(persist)
      case _ => None
    }
  }

}
