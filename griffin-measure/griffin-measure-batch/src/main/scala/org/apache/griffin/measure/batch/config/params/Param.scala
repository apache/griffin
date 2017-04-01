package org.apache.griffin.measure.batch.config.params

trait Param extends Serializable {

  def validate(): Boolean = true
  
}
