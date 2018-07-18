/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.context.streaming.offset

import org.apache.griffin.measure.configuration.dqdefinition.OffsetCacheParam

import scala.util.{Success, Try}

case class OffsetCacheFactory(offsetCacheParams: Iterable[OffsetCacheParam], metricName: String
                             ) extends Serializable {

  val ZK_REGEX = """^(?i)zk|zookeeper$""".r

  def getOffsetCache(offsetCacheParam: OffsetCacheParam): Option[OffsetCache] = {
    val config = offsetCacheParam.getConfig
    val offsetCacheTry = offsetCacheParam.getType match {
      case ZK_REGEX() => Try(OffsetCacheInZK(config, metricName))
      case _ => throw new Exception("not supported info cache type")
    }
    offsetCacheTry.toOption
  }

}