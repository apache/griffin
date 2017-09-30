/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.utils

import org.apache.griffin.measure.log.Loggable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

object HdfsUtil extends Loggable {

  private val seprator = "/"

  private val conf = new Configuration()
  conf.set("dfs.support.append", "true")
//  conf.set("fs.defaultFS", "hdfs://localhost")    // debug @localhost

  private val dfs = FileSystem.get(conf)

  def existPath(filePath: String): Boolean = {
    try {
      val path = new Path(filePath)
      dfs.exists(path)
    } catch {
      case e: Throwable => false
    }
  }

  def existFileInDir(dirPath: String, fileName: String): Boolean = {
    val filePath = getHdfsFilePath(dirPath, fileName)
    existPath(filePath)
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
    try {
      val path = new Path(dirPath)
      if (dfs.exists(path)) dfs.delete(path, true)
    } catch {
      case e: Throwable => error(s"delete path [${dirPath}] error: ${e.getMessage}")
    }
  }

//  def listPathFiles(dirPath: String): Iterable[String] = {
//    val path = new Path(dirPath)
//    try {
//      val fileStatusArray = dfs.listStatus(path)
//      fileStatusArray.flatMap { fileStatus =>
//        if (fileStatus.isFile) {
//          Some(fileStatus.getPath.getName)
//        } else None
//      }
//    } catch {
//      case e: Throwable => {
//        println(s"list path files error: ${e.getMessage}")
//        Nil
//      }
//    }
//  }

  def listSubPathsByType(dirPath: String, subType: String, fullPath: Boolean = false): Iterable[String] = {
    if (existPath(dirPath)) {
      try {
        val path = new Path(dirPath)
        val fileStatusArray = dfs.listStatus(path)
        fileStatusArray.filter { fileStatus =>
          subType match {
            case "dir" => fileStatus.isDirectory
            case "file" => fileStatus.isFile
            case _ => true
          }
        }.map { fileStatus =>
          val fname = fileStatus.getPath.getName
          if (fullPath) getHdfsFilePath(dirPath, fname) else fname
        }
      } catch {
        case e: Throwable => {
          warn(s"list path [${dirPath}] warn: ${e.getMessage}")
          Nil
        }
      }
    } else Nil
  }

  def listSubPathsByTypes(dirPath: String, subTypes: Iterable[String], fullPath: Boolean = false): Iterable[String] = {
    subTypes.flatMap { subType =>
      listSubPathsByType(dirPath, subType, fullPath)
    }
  }

  def fileNameFromPath(filePath: String): String = {
    val path = new Path(filePath)
    path.getName
  }
}
