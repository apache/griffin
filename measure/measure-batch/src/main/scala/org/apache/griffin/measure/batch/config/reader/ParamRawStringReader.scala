package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.Param
import org.apache.griffin.measure.batch.utils.JsonUtil

import scala.util.Try

case class ParamRawStringReader(rawString: String) extends ParamReader {

  def readConfig[T <: Param](implicit m : Manifest[T]): Try[T] = {
    Try {
      val param = JsonUtil.fromJson[T](rawString)
      param
    }
  }

}
