package org.apache.griffin.record.result

case class AccuResult(missCount: Long, totalCount: Long) extends Result {

  type T = AccuResult

  def updateResult(delta: T): T = {
    AccuResult(delta.missCount, totalCount)
  }

  def needCalc(): Boolean = {
    this.missCount > 0
  }

  def differsFrom(other: T): Boolean = {
    (this.missCount != other.missCount) || (this.totalCount != other.totalCount)
  }
}
