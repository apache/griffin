package org.apache.griffin.measure.batch.result

// result for profile: match count, total count
case class ProfileResult(matchCount: Long, totalCount: Long) extends Result {

  type T = ProfileResult

  def update(delta: T): T = {
    ProfileResult(matchCount + delta.matchCount, totalCount)
  }

  def eventual(): Boolean = {
    this.matchCount >= totalCount
  }

  def differsFrom(other: T): Boolean = {
    (this.matchCount != other.matchCount) || (this.totalCount != other.totalCount)
  }

  def getMiss = totalCount - matchCount
  def getTotal = totalCount
  def getMatch = matchCount

  def matchPercentage: Double = if (getTotal <= 0) 0 else getMatch.toDouble / getTotal * 100

}
