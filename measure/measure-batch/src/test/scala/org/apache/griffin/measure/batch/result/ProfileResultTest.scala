package org.apache.griffin.measure.batch.result

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ProfileResultTest extends FunSuite with BeforeAndAfter with Matchers {

  test ("update") {
    val result = ProfileResult(10, 100)
    val delta = ProfileResult(30, 90)
    result.update(delta) should be (ProfileResult(40, 100))
  }

  test ("eventual") {
    val result1 = ProfileResult(10, 100)
    result1.eventual should be (false)

    val result2 = ProfileResult(100, 100)
    result2.eventual should be (true)
  }

  test ("differsFrom") {
    val result = ProfileResult(10, 100)
    result.differsFrom(ProfileResult(11, 100)) should be (true)
    result.differsFrom(ProfileResult(10, 110)) should be (true)
    result.differsFrom(ProfileResult(10, 100)) should be (false)
  }

  test ("matchPercentage") {
    val result1 = ProfileResult(90, 100)
    result1.matchPercentage should be (90.0)

    val result2 = ProfileResult(10, 0)
    result2.matchPercentage should be (0.0)
  }

}
