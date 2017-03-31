//package org.apache.griffin.config.reader
//
//import org.apache.griffin.config.params._
//import org.apache.griffin.utils.JsonUtil
//
//class ConfigFileReader(file: String) extends ConfigReader {
//
//  def readConfig: AllParam = {
//    val lines = scala.io.Source.fromFile(file).mkString
//    val param = JsonUtil.fromJson2AllParam(lines)
//    param
//  }
//
//}
