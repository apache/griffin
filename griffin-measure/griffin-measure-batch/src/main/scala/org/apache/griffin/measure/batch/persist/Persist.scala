package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.log.Loggable
import org.apache.griffin.measure.batch.result._


trait Persist extends Loggable with Serializable {
  val timeStamp: Long

  def start(msg: String): Unit
  def finish(): Unit

  def result(rt: Long, result: Result): Unit

  def missRecords(records: Iterable[AnyRef]): Unit

  def log(rt: Long, msg: String): Unit
}
