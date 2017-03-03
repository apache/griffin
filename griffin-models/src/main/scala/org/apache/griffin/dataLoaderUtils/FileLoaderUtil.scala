package org.apache.griffin.dataLoaderUtils

object FileLoaderUtil {
  def convertPath(path: String) : String = {
    path.replace("/", System.getProperty("file.separator"))
  }
}
