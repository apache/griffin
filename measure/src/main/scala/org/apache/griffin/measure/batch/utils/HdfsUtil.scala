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
