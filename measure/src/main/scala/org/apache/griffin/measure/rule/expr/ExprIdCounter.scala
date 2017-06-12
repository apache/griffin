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
package org.apache.griffin.measure.rule.expr

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{Set => MutableSet}

object ExprIdCounter {

  private val idCounter: AtomicLong = new AtomicLong(0L)

  private val existIdSet: MutableSet[String] = MutableSet.empty[String]

  private val invalidIdRegex = """^\d+$""".r

  val emptyId: String = ""

  def genId(defaultId: String): String = {
    defaultId match {
      case emptyId => increment.toString
      case invalidIdRegex() => increment.toString
//      case defId if (exist(defId)) => s"${increment}#${defId}"
      case defId if (exist(defId)) => s"${defId}"
      case _ => {
        insertUserId(defaultId)
        defaultId
      }
    }
  }

  private def exist(id: String): Boolean = {
    existIdSet.contains(id)
  }

  private def insertUserId(id: String): Unit = {
    existIdSet += id
  }

  private def increment(): Long = {
    idCounter.incrementAndGet()
  }

}
