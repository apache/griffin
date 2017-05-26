package org.apache.griffin.measure.batch.persist

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class HttpPersistTest extends FunSuite with Matchers with BeforeAndAfter {

  val config: Map[String, Any] = Map[String, Any](("api" -> "url/api"), ("method" -> "post"))
  val metricName: String = "metric"
  val timeStamp: Long = 123456789L

  val httpPersist = HttpPersist(config, metricName, timeStamp)

  test ("constructor") {
    httpPersist.api should be ("url/api")
    httpPersist.method should be ("post")
  }

  test("available") {
    httpPersist.available should be (true)
  }
}
