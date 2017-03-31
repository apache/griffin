package org.apache.griffin.measure.batch.config.params

trait ConfigParam extends Serializable {

  def validate(): Boolean = true
  
}
