package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.env._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ParamRawStringReaderTest extends FunSuite with Matchers with BeforeAndAfter {

  test("read config") {
    val rawString = """{"type": "hdfs", "config": {"path": "/path/to", "time": 1234567}}"""

    val reader = ParamRawStringReader(rawString)
    val paramTry = reader.readConfig[PersistParam]
    paramTry.isSuccess should be (true)
    paramTry.get should be (PersistParam("hdfs", Map[String, Any](("path" -> "/path/to"), ("time" -> 1234567))))
  }

}
