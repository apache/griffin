package org.apache.griffin.measure.batch.persist

import org.apache.griffin.measure.batch.log.Loggable
import org.apache.griffin.measure.batch.result.AccuracyResult
import org.apache.griffin.measure.batch.utils.JsonUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.Try


@RunWith(classOf[JUnitRunner])
class HttpPersistTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {

  test("test result json") {
    val ar = AccuracyResult(10, 1000)
    val dataMap = Map[String, Any](("name" -> "metric"), ("tmst" -> System.currentTimeMillis), ("total" -> ar.getTotal), ("matched" -> ar.getMatch))
    val data = JsonUtil.toJson(dataMap)
    println(data)
  }

  test("test try") {
    Try {
      ;
    }

  }

}
