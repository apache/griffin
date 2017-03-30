package org.apache.griffin.config

import org.apache.griffin.config.params.ConfigParam

trait ConfigReader extends Serializable {

//  def readConfig[T <: ConfigParam](implicit m : Manifest[T]): T

  def readConfig: ConfigParam

}
