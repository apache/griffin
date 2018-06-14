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

trait OffsetKeys extends Serializable {

  val CacheTime = "cache.time"
  val LastProcTime = "last.proc.time"
  val ReadyTime = "ready.time"
  val CleanTime = "clean.time"
  val OldCacheIndex = "old.cache.index"

  def cacheTime(path: String): String = s"${path}/${CacheTime}"
  def lastProcTime(path: String): String = s"${path}/${LastProcTime}"
  def readyTime(path: String): String = s"${path}/${ReadyTime}"
  def cleanTime(path: String): String = s"${path}/${CleanTime}"
  def oldCacheIndex(path: String): String = s"${path}/${OldCacheIndex}"

  val infoPath = "info"

  val finalCacheInfoPath = "info.final"
  val finalReadyTime = s"${finalCacheInfoPath}/${ReadyTime}"
  val finalLastProcTime = s"${finalCacheInfoPath}/${LastProcTime}"
  val finalCleanTime = s"${finalCacheInfoPath}/${CleanTime}"

}
