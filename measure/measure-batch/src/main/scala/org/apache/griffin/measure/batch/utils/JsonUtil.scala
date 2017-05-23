package org.apache.griffin.measure.batch.utils

import java.io.InputStream

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect._

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

  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T: ClassTag](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  def fromJson[T: ClassTag](is: InputStream)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](is, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }
}
