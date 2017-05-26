package org.apache.griffin.measure.batch.config.validator

import org.apache.griffin.measure.batch.config.params.Param

import scala.util.Try

// need to validate params
case class AllParamValidator() extends ParamValidator {

  def validate[T <: Param](param: Param): Try[Boolean] = {
    Try {
      param.validate
    }
  }

}
