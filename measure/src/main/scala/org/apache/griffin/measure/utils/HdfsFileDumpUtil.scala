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

import org.apache.spark.rdd.RDD

object HdfsFileDumpUtil {

  val sepCount = 5000

  private def suffix(i: Long): String = {
    if (i == 0) "" else s".${i}"
  }
  private def samePattern(fileName: String, patternFileName: String): Boolean = {
    fileName.startsWith(patternFileName)
  }

  def splitRdd[T](rdd: RDD[T])(implicit m: Manifest[T]): RDD[(Long, Iterable[T])] = {
    val indexRdd = rdd.zipWithIndex
    indexRdd.map(p => ((p._2 / sepCount), p._1)).groupByKey()
  }
  def splitIterable[T](datas: Iterable[T])(implicit m: Manifest[T]): Iterator[(Int, Iterable[T])] = {
    val groupedData = datas.grouped(sepCount).zipWithIndex
    groupedData.map(v => (v._2, v._1))
  }

  private def directDump(path: String, list: Iterable[String], lineSep: String): Unit = {
    // collect and save
    val strRecords = list.mkString(lineSep)
    // save into hdfs
    HdfsUtil.writeContent(path, strRecords)
  }

  def dump(path: String, recordsRdd: RDD[String], lineSep: String): Boolean = {
    val groupedRdd = splitRdd(recordsRdd)
    groupedRdd.aggregate(true)({ (res, pair) =>
      val (idx, list) = pair
      val filePath = path + suffix(idx)
      directDump(filePath, list, lineSep)
      true
    }, _ && _)
  }
  def dump(path: String, records: Iterable[String], lineSep: String): Boolean = {
    val groupedRecords = splitIterable(records)
    groupedRecords.aggregate(true)({ (res, pair) =>
      val (idx, list) = pair
      val filePath = path + suffix(idx)
      directDump(filePath, list, lineSep)
      true
    }, _ && _)
  }

  def remove(path: String, filename: String, withSuffix: Boolean): Unit = {
    if (withSuffix) {
      val files = HdfsUtil.listSubPathsByType(path, "file")
      val patternFiles = files.filter(samePattern(_, filename))
      patternFiles.foreach { f =>
        val rmPath = HdfsUtil.getHdfsFilePath(path, f)
        HdfsUtil.deleteHdfsPath(rmPath)
      }
    } else {
      val rmPath = HdfsUtil.getHdfsFilePath(path, filename)
      HdfsUtil.deleteHdfsPath(rmPath)
    }
  }

}
