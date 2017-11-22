package org.apache.griffin.measure.cache.tmst

import java.util.concurrent.atomic.AtomicLong

import org.apache.griffin.measure.log.Loggable

import scala.collection.mutable.{SortedSet => MutableSortedSet}


object TmstCache extends Loggable {

  private val tmstGroup: MutableSortedSet[Long] = MutableSortedSet.empty[Long]

  //-- insert tmst into tmst group --
  def insert(tmst: Long) = tmstGroup += tmst
  def insert(tmsts: Iterable[Long]) = tmstGroup ++= tmsts

  //-- remove tmst from tmst group --
  def remove(tmst: Long) = tmstGroup -= tmst
  def remove(tmsts: Iterable[Long]) = tmstGroup --= tmsts

  //-- get subset of tmst group --
  def range(from: Long, until: Long) = tmstGroup.range(from, until).toSet
  def until(until: Long) = tmstGroup.until(until).toSet
  def from(from: Long) = tmstGroup.from(from).toSet
  def all = tmstGroup.toSet

  //-- df name --
  private val tmstNameRegex = """^(.*)\[(\d*)\]\((\d*)\)$""".r
  def tmstName(name: String, tmst: Long, groupId: Long) = s"${name}[${tmst}](${groupId})"
  def extractTmstName(tmstName: String): (String, Option[Long]) = {
    tmstName match {
      case tmstNameRegex(name, tmst, groupId) => {
        try { (name, Some(tmst.toLong)) } catch { case e: Throwable => (tmstName, None) }
      }
      case _ => (tmstName, None)
    }
  }

}

object CalcGroupGenerator {
  private val counter: AtomicLong = new AtomicLong(0L)

  def genId: Long = increment

  private def increment: Long = {
    counter.incrementAndGet()
  }
}