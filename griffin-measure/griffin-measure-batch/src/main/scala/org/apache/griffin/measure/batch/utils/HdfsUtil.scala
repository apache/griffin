package org.apache.griffin.measure.batch.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

object HdfsUtil {

  private val seprator = "/"

  private val conf = new Configuration()
  conf.set("dfs.support.append", "true")

  private val dfs = FileSystem.get(conf)

  def existPath(filePath: String): Boolean = {
    val path = new Path(filePath)
    dfs.exists(path)
  }

  def createFile(filePath: String): FSDataOutputStream = {
    val path = new Path(filePath)
    if (dfs.exists(path)) dfs.delete(path, true)
    return dfs.create(path)
  }

  def appendOrCreateFile(filePath: String): FSDataOutputStream = {
    val path = new Path(filePath)
    if (dfs.exists(path)) dfs.append(path) else createFile(filePath)
  }

  def openFile(filePath: String): FSDataInputStream = {
    val path = new Path(filePath)
    dfs.open(path)
  }

  def writeContent(filePath: String, message: String): Unit = {
    val out = createFile(filePath)
    out.write(message.getBytes("utf-8"))
    out.close
  }

  def appendContent(filePath: String, message: String): Unit = {
    val out = appendOrCreateFile(filePath)
    out.write(message.getBytes("utf-8"))
    out.close
  }

  def createEmptyFile(filePath: String): Unit = {
    val out = createFile(filePath)
    out.close
  }


  def getHdfsFilePath(parentPath: String, fileName: String): String = {
    if (parentPath.endsWith(seprator)) parentPath + fileName else parentPath + seprator + fileName
  }

  def deleteHdfsPath(dirPath: String): Unit = {
    val path = new Path(dirPath)
    if (dfs.exists(path)) dfs.delete(path, true)
  }
}
