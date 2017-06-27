package org.apache.griffin.measure.connector

import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.reader.ParamRawStringReader
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class ConnectorTest extends FunSuite with Matchers with BeforeAndAfter {

  test("read config") {

    val a = "java.lang.String"
    val at = getClassTag(a)
    println(at)

    at match {
      case ClassTag(m) => println(m)
      case _ => println("no")
    }

  }

  private def getClassTag(tp: String): ClassTag[_] = {
    val clazz = Class.forName(tp)
    ClassTag(clazz)
  }

//  private def getDeserializer(ct: ClassTag[_]): String = {
//    ct.runtimeClass.get
//    ct match {
//      case Some(t: scala.Predef.Class[String]) => "kafka.serializer.StringDecoder"
//    }
//  }

}

