package org.apache.griffin.measure.batch.config.validator

import org.apache.griffin.measure.batch.config.params.Param

import scala.util.Try

case class AllParamValidator() extends ParamValidator {

  def validate[T <: Param](param: Param): Try[Boolean] = {
    Try {
      true
    }
  }

}
