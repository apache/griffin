package org.apache.griffin.record

import org.apache.griffin.record.result.AccuResult


abstract class Recorder(time: Long) extends Serializable {

  def getTime(): Long = time

  def start(): Unit
  def finish(): Unit
  def error(rt: Long, msg: String): Unit
  def info(rt: Long, msg: String): Unit

  def accuracyResult(rt: Long, res: AccuResult): Unit
  def accuracyMissingRecords(records: Iterable[AnyRef]): Unit

  def recordTime(rt: Long, calcTime: Long): Unit
}
