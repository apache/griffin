package org.apache.griffin.measure.batch.config.validator

import org.apache.griffin.measure.batch.config.params._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory

@RunWith(classOf[JUnitRunner])
class AllParamValidatorTest extends FlatSpec with Matchers with BeforeAndAfter with MockFactory {

  "validate" should "pass" in {
    val validator = AllParamValidator()
    val paramMock = mock[Param]
    paramMock.validate _ expects () returning (false)

    val validateTry = validator.validate(paramMock)
    validateTry.isSuccess should be (true)
    validateTry.get should be (false)
  }

}
