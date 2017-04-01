package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.Param

import scala.util.Try

trait ParamReader extends Serializable {

  def readConfig[T <: Param](implicit m : Manifest[T]): Try[T]

}
