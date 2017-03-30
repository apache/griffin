package org.apache.griffin.config.params


trait ConfigParam extends Serializable {

  def merge(cp: ConfigParam): Unit = {}

  def validate(): Boolean = true

}
