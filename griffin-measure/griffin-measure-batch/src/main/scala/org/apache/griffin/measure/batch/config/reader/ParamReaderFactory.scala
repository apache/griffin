package org.apache.griffin.measure.batch.config.reader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object ParamReaderFactory {

  val LocalFsRegex = """^(?i)local$""".r
  val HdfsFsRegex = """^(?i)hdfs$""".r

  def getParamReader(filePath: String, fsType: String): ParamReader = {
    fsType match {
      case LocalFsRegex() => ParamFileReader(filePath)
      case HdfsFsRegex() => ParamHdfsFileReader(filePath)
      case _ => ParamHdfsFileReader(filePath)
    }
  }

}
