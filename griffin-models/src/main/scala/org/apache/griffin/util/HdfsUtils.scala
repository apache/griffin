package org.apache.griffin.util

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

object HdfsUtils {

  private val conf = new Configuration()

  private val dfs = FileSystem.get(conf)

  def createFile(filePath: String): FSDataOutputStream = {
    return dfs.create(new Path(filePath))
  }

  def openFile(filePath: String): FSDataInputStream = {
    return dfs.open(new Path(filePath))
  }

  def writeFile(filePath: String, message: String): Unit = {
    val out = createFile(filePath)
    out.write(message.getBytes("utf-8"))
  }

}