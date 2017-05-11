package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.result._
import org.apache.griffin.measure.batch.utils.{HttpUtil, JsonUtil}
import org.apache.spark.rdd.RDD

import scala.util.Try

case class MultiPersists(persists: Iterable[Persist]) extends Persist {

  val timeStamp: Long = persists match {
    case Nil => 0
    case _ => persists.head.timeStamp
  }

  val config: Map[String, Any] = Map[String, Any]()

  def available(): Boolean = { persists.exists(_.available()) }

  def start(msg: String): Try[Unit] = Try { persists.foreach(_.start(msg)) }
  def finish(): Try[Unit] = Try { persists.foreach(_.finish()) }

  def result(rt: Long, result: Result): Try[Unit] = Try { persists.foreach(_.result(rt, result)) }

  def missRecords(records: RDD[String]): Try[Unit] = Try { persists.foreach(_.missRecords(records)) }

  def log(rt: Long, msg: String): Try[Unit] = Try { persists.foreach(_.log(rt, msg)) }

}
