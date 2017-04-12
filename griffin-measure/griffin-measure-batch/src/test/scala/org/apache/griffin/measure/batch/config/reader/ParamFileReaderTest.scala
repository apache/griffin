package org.apache.griffin.measure.batch.config.reader

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.config.params.env._

import scala.util.{Failure, Success}
import org.apache.griffin.measure.batch.log.Loggable
import org.apache.griffin.measure.batch.persist.{HdfsPersist, PersistFactory}


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

    if (p2.isSuccess) {
      val persist = PersistFactory(p2.get.persistParams, "test").getPersists(123456L)
      for (elem <- persist.persists) {
        elem match {
          case ele: HdfsPersist => {
            println(ele.maxPersistLines)
            println(ele.maxLinesPerFile)
          }
          case _ => println("")
        }
      }
    }
  }

}
