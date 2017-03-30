package org.apache.griffin.config

import org.apache.griffin.config.params._
import org.apache.griffin.utils.JsonUtil

case class ConfigFileReader(file: String) extends ConfigReader {

//  def readConfig[T <: ConfigParam](implicit m : Manifest[T]): T = {
//    val lines = scala.io.Source.fromFile(file).mkString
//    val param = JsonUtil.fromJson[T](lines)
//    param
//  }

  def readConfig: AllParam = {
    val lines = scala.io.Source.fromFile(file).mkString
    val param = JsonUtil.fromJson2AllParam(lines)
    param
  }

}
