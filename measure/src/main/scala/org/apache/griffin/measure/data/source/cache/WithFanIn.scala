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
package org.apache.griffin.measure.data.source.cache

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.{TrieMap, Map => ConcMap}

trait WithFanIn[T] {

  val totalNum: AtomicInteger = new AtomicInteger(0)
  val fanInCountMap: ConcMap[T, Int] = TrieMap[T, Int]()

  def registerFanIn(): Int = {
    totalNum.incrementAndGet()
  }

  def fanIncrement(key: T): Boolean = {
    fanInc(key)
    fanInCountMap.get(key) match {
      case Some(n) if (n >= totalNum.get) => {
        fanInCountMap.remove(key)
        true
      }
      case _ => false
    }
  }

  private def fanInc(key: T): Unit = {
    fanInCountMap.get(key) match {
      case Some(n) => {
        val suc = fanInCountMap.replace(key, n, n + 1)
        if (!suc) fanInc(key)
      }
      case _ => {
        val oldOpt = fanInCountMap.putIfAbsent(key, 1)
        if (oldOpt.nonEmpty) fanInc(key)
      }
    }
  }

}
