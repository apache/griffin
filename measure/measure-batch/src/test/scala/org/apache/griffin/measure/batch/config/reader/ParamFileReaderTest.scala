package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.env._
import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.log.Loggable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Failure, Success}


@RunWith(classOf[JUnitRunner])
class ParamFileReaderTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {

  test("test file reader") {
    val userReader = ParamFileReader("src/test/resources/config1.json")
    val envReader = ParamFileReader("src/test/resources/env1.json")

    val p1 = userReader.readConfig[UserParam]
    val p2 = envReader.readConfig[EnvParam]

    p1 match {
      case Success(v) => println(v)
      case Failure(ex) => error(ex.getMessage)
    }

    p2 match {
      case Success(v) => println(v)
      case Failure(ex) => error(ex.getMessage)
    }
  }

}
