package org.apache.griffin.measure.batch.result

// result for accuracy: miss count, total count
case class AccuracyResult(miss: Long, total: Long) extends Result {

  type T = AccuracyResult

  def update(delta: T): T = {
    AccuracyResult(delta.miss, total)
  }

  def eventual(): Boolean = {
    this.miss <= 0
  }

  def differsFrom(other: T): Boolean = {
    (this.miss != other.miss) || (this.total != other.total)
  }

  def getMiss = miss
  def getTotal = total
  def getMatch = total - miss

  def matchPercentage: Double = if (getTotal <= 0) 0 else getMatch.toDouble / getTotal * 100

}
