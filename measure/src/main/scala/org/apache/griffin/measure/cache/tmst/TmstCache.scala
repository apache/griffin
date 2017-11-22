package org.apache.griffin.measure.cache.tmst

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

}
