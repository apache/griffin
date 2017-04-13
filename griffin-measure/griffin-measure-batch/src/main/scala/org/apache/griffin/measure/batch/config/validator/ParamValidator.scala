package org.apache.griffin.measure.batch.config.validator

import org.apache.griffin.measure.batch.log.Loggable
import org.apache.griffin.measure.batch.config.params.Param

import scala.util.Try

trait ParamValidator extends Loggable with Serializable {

  def validate[T <: Param](param: Param): Try[Boolean]

}
