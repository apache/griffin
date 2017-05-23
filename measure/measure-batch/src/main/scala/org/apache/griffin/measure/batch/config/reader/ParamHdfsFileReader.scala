package org.apache.griffin.measure.batch.config.reader

import org.apache.griffin.measure.batch.config.params.Param
import org.apache.griffin.measure.batch.utils.JsonUtil
import org.apache.griffin.measure.batch.utils.HdfsUtil

import scala.util.Try

case class ParamHdfsFileReader(filePath: String) extends ParamReader {

  def readConfig[T <: Param](implicit m : Manifest[T]): Try[T] = {
    Try {
      val source = HdfsUtil.openFile(filePath)
      val param = JsonUtil.fromJson[T](source)
      source.close
      param
    }
  }

}
