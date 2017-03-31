package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.ConfigParam

trait ConfigReader extends Serializable {

  def readConfig: ConfigParam

}
