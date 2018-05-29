package org.apache.griffin.measure

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class ApplicationTest extends FunSuite with Matchers with BeforeAndAfter {

  val envFile = "src/test/resources/env-batch.json"
//  val envFile = "src/test/resources/env-streaming.json"

//  val confFile = "src/test/resources/config.json"
  val confFile = "src/test/resources/config-griffindsl.json"
//  val confFile = "src/test/resources/config-streaming.json"
//    val confFile = "src/test/resources/config-streaming-accuracy.json"

  test("test application") {
    val args = Array[String](envFile, confFile)
    Application.main(args)
  }

}
