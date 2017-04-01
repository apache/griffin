package org.apache.griffin.measure.batch.config.reader

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.config.params.env._


@RunWith(classOf[JUnitRunner])
class ParamFileReaderTest extends FunSuite with Matchers with BeforeAndAfter {

  test("test file reader") {
    val userReader = ParamFileReader("src/test/resources/config.json")
    val envReader = ParamFileReader("src/test/resources/env.json")

    val p1 = userReader.readConfig[UserParam]
    val p2 = envReader.readConfig[EnvParam]

    println(p1)
    println(p2)
  }

}
