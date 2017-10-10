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

import java.io.{BufferedReader, FileReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class HdfsUtilTest extends FunSuite with Matchers with BeforeAndAfter {

  private val seprator = "/"

  private val conf1 = new Configuration()
  conf1.addResource(new Path("file:///apache/hadoop/etc/hadoop/core-site.xml"))
  conf1.addResource(new Path("file:///apache/hadoop/etc/hadoop/hdfs-site.xml"))
  private val dfs1 = FileSystem.get(conf1)

  private val conf2 = new Configuration()
  conf2.addResource(new Path("file:///Users/lliu13/test/hadoop/core-site.xml"))
  conf2.addResource(new Path("file:///Users/lliu13/test/hadoop/hdfs-site.xml"))
  private val dfs2 = FileSystem.get(conf2)

//  val conf = new SparkConf().setAppName("test_hdfs").setMaster("local[*]")
//  val sparkContext = new SparkContext(conf)
//  sparkContext.setLogLevel("WARN")
//  val sqlContext = new HiveContext(sparkContext)

  def listSubPaths(dfs: FileSystem, dirPath: String, subType: String, fullPath: Boolean = false): Iterable[String] = {
    val path = new Path(dirPath)
    try {
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
        println(s"list path files error: ${e.getMessage}")
        Nil
      }
    }
  }

  def getHdfsFilePath(parentPath: String, fileName: String): String = {
    if (parentPath.endsWith(seprator)) parentPath + fileName else parentPath + seprator + fileName
  }

//  test ("test multiple hdfs") {
//    val list1 = listSubPaths(dfs1, "/", "dir", false)
//    println(list1)
//
//    val list2 = listSubPaths(dfs2, "/", "dir", false)
//    println(list2)
//
//    val path1 = "/depth/discovery_file_sample.txt"
//    val istream1 = dfs1.open(new Path(path1))
//    val reader1 = new BufferedReader(new InputStreamReader(istream1))
//    val seq1 = scala.collection.mutable.MutableList[String]()
//    try {
//      var line = reader1.readLine()
//      while (line != null) {
//        val arr = line.split("\u0007")
//        seq1 ++= arr
//        line = reader1.readLine()
//      }
//    } finally {
//      reader1.close()
//      istream1.close()
//    }
//
////    val scanner = new java.util.Scanner(istream1,"UTF-8").useDelimiter("\u0007")
////    val theString = if (scanner.hasNext()) scanner.next() else ""
////    println(theString)
////    scanner.close()
//
//    println(seq1.size)
//    println(seq1.take(10))
//    seq1.take(10).foreach(println)
//
////    val path2 = "/griffin/json/env.json"
////    val istream2 = dfs2.open(new Path(path2))
////    val reader2 = new BufferedReader(new InputStreamReader(istream2))
////    val seq2 = scala.collection.mutable.MutableList[String]()
////    try {
////      var line = reader2.readLine()
////      while (line != null) {
////        line = reader2.readLine()
////        seq2 += line
////      }
////    } catch {
////      case e: Throwable => {
////        println("error in reading")
////      }
////    } finally {
////      reader2.close()
////      istream2.close()
////    }
////    println(seq2.size)
////    println(seq2.take(10))
//  }

}
