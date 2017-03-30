package org.apache.griffin.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.griffin.config.params.AllParam

object JsonUtil {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

//  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)
//
//  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
//    mapper.readValue(json, classOf[T])
//  }

  def toAnyMap(json: String) = {
    mapper.readValue(json, classOf[Map[String, Any]])
  }

  def fromJson2AllParam(json: String) = {
    mapper.readValue(json, classOf[AllParam])
  }
}
