package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.log.Loggable
import org.apache.griffin.measure.batch.result._
import org.apache.spark.rdd.RDD

import scala.util.Try


trait Persist extends Loggable with Serializable {
  val timeStamp: Long

  val config: Map[String, Any]

  def available(): Boolean

  def start(msg: String): Try[Unit]
  def finish(): Try[Unit]

  def result(rt: Long, result: Result): Try[Unit]

  def missRecords(records: RDD[String]): Try[Unit]

  def log(rt: Long, msg: String): Try[Unit]
}
