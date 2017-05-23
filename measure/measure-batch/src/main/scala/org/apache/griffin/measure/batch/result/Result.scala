package org.apache.griffin.measure.batch.result


trait Result extends Serializable {

  type T <: Result

  def update(delta: T): T

  def eventual(): Boolean

  def differsFrom(other: T): Boolean

}
