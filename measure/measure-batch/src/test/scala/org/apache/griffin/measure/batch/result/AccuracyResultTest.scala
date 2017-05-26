package org.apache.griffin.measure.batch.result

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class AccuracyResultTest extends FunSuite with BeforeAndAfter with Matchers {

  test ("update") {
    val result = AccuracyResult(10, 100)
    val delta = AccuracyResult(3, 10)
    result.update(delta) should be (AccuracyResult(3, 100))
  }

  test ("eventual") {
    val result1 = AccuracyResult(10, 100)
    result1.eventual should be (false)

    val result2 = AccuracyResult(0, 100)
    result2.eventual should be (true)
  }

  test ("differsFrom") {
    val result = AccuracyResult(10, 100)
    result.differsFrom(AccuracyResult(11, 100)) should be (true)
    result.differsFrom(AccuracyResult(10, 110)) should be (true)
    result.differsFrom(AccuracyResult(10, 100)) should be (false)
  }

  test ("matchPercentage") {
    val result1 = AccuracyResult(10, 100)
    result1.matchPercentage should be (90.0)

    val result2 = AccuracyResult(10, 0)
    result2.matchPercentage should be (0.0)
  }

}
