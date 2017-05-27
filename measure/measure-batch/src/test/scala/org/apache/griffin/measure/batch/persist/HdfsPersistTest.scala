package org.apache.griffin.measure.batch.persist

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Try, Failure}

@RunWith(classOf[JUnitRunner])
class HdfsPersistTest extends FunSuite with Matchers with BeforeAndAfter {

  val config: Map[String, Any] = Map[String, Any](
    ("path" -> "/path/to"), ("max.persist.lines" -> 100), ("max.lines.per.file" -> 1000))
  val metricName: String = "metric"
  val timeStamp: Long = 123456789L

  val hdfsPersist = HdfsPersist(config, metricName, timeStamp)

  test ("constructor") {
    hdfsPersist.path should be ("/path/to")
    hdfsPersist.maxPersistLines should be (100)
    hdfsPersist.maxLinesPerFile should be (1000)

    hdfsPersist.StartFile should be (s"/path/to/${metricName}/${timeStamp}/_START")
  }

  test ("avaiable") {
    hdfsPersist.available should be (true)
  }
}
