package org.apache.griffin.config

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.griffin.utils.JsonUtil

object ConfigReadTest {

  def main(args: Array[String]): Unit = {
//    val originalMap = Map("a" -> List(1,2), "b" -> List(3,4,5), "c" -> List())
//    val json = JsonUtil.toJson(originalMap)
//    val map = JsonUtil.toMap[Seq[Int]](json)
//
//    println(json)
//    println(map)
//
//    val mutableSymbolMap = JsonUtil.fromJson[collection.mutable.Map[Symbol,Seq[Int]]](json)
//    println(mutableSymbolMap)
//
//
//
//    val jeroen = Person("Jeroen", 26)
//    val martin = Person("Martin", 54)
//
//    val originalGroup = Group(null, Seq(jeroen,martin), martin)
//
//    val groupJson = JsonUtil.toJson(originalGroup)
//
//    val group = JsonUtil.fromJson[Group](groupJson)
//
//    println(originalGroup)
//    println(groupJson)
//    println(group)
//
//    val groupJson1 = """{"prs":[{"name":"Jeroen","age":26}]}"""
//    val group1 = JsonUtil.fromJson[Group](groupJson1)
//    println(group1)
  }

}

case class Person(name: String, age: Int)

@JsonInclude(Include.NON_NULL)
case class Group(name: String, @JsonProperty("prs") persons: Seq[Person], leader: Person)