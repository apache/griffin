package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.Param
import org.apache.griffin.measure.batch.utils.JsonUtil

import scala.util.Try

case class ParamFileReader(file: String) extends ParamReader {

  def readConfig[T <: Param](implicit m : Manifest[T]): Try[T] = {
    Try {
      val lines = scala.io.Source.fromFile(file).mkString
      val param = JsonUtil.fromJson[T](lines)
      param
    }
  }

}
