package org.apache.griffin.config

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

import org.apache.griffin.config.params._


@RunWith(classOf[JUnitRunner])
class ConfigFileReaderTest extends FunSuite with Matchers with BeforeAndAfter {

  test("test file reader") {
    val reader: ConfigReader = ConfigFileReader("src/main/resources/config.json")
//    val param = reader.readConfig[AllParam]
    val param = reader.readConfig

    println(param)
  }

}
