package org.apache.griffin.record.result

trait Result extends Serializable {

  type T <: Result

  def updateResult(delta: T): T

  def needCalc(): Boolean

  def differsFrom(other: T): Boolean
}
