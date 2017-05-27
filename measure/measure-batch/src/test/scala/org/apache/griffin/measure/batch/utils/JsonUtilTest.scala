package org.apache.griffin.measure.batch.utils

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}


@RunWith(classOf[JUnitRunner])
class JsonUtilTest extends FunSuite with Matchers with BeforeAndAfter {

  val map = Map[String, Any](("name" -> "test"), ("age" -> 15))
  val json = """{"name":"test","age":15}"""

  val person = JsonUtilTest.Person("test", 15)

  test ("toJson 1") {
    val symbolMap = map.map(p => (Symbol(p._1), p._2))
    JsonUtil.toJson(symbolMap) should equal (json)
  }

  test ("toJson 2") {
    JsonUtil.toJson(map) should equal (json)
  }

  test ("toMap") {
    JsonUtil.toMap(json) should equal (map)
  }

  test ("fromJson 1") {
    JsonUtil.fromJson[JsonUtilTest.Person](json) should equal (person)
  }

  test ("fromJson 2") {
    val is = new java.io.ByteArrayInputStream(json.getBytes("utf-8"));
    JsonUtil.fromJson[JsonUtilTest.Person](is) should equal (person)
  }

}

object JsonUtilTest {
  case class Person(name: String, age: Int){}
}
