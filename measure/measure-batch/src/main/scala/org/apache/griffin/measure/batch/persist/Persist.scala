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

import org.apache.griffin.measure.batch.log.Loggable
import org.apache.griffin.measure.batch.result._
import org.apache.spark.rdd.RDD

import scala.util.Try


trait Persist extends Loggable with Serializable {
  val timeStamp: Long

  val config: Map[String, Any]

  def available(): Boolean

  def start(msg: String): Unit
  def finish(): Unit

  def result(rt: Long, result: Result): Unit

  def missRecords(records: RDD[String]): Unit
  def matchRecords(records: RDD[String]): Unit

  def log(rt: Long, msg: String): Unit
}
