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
package org.apache.griffin.measure.process.temp

import org.apache.griffin.measure.log.Loggable
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.concurrent.{TrieMap, Map => ConcMap}

object TableRegisters extends Loggable {

  final val _global = "_global"
//
//  val tables: ConcMap[String, Set[String]] = TrieMap[String, Set[String]]()

  val compileTableRegs = TableRegs()
  val runTableRegs = TableRegs()

//  private def registerTable(key: String, table: String): Unit = {
//    tables.get(key) match {
//      case Some(set) => {
//        val suc = tables.replace(key, set, set + table)
//        if (!suc) registerTable(key, table)
//      }
//      case _ => {
//        val oldOpt = tables.putIfAbsent(key, Set[String](table))
//        if (oldOpt.nonEmpty) registerTable(key, table)
//      }
//    }
//  }
//
//  private def unregisterTable(key: String, table: String): Option[String] = {
//    tables.get(key) match {
//      case Some(set) => {
//        val ftb = set.find(_ == table)
//        ftb match {
//          case Some(tb) => {
//            val nset = set - tb
//            val suc = tables.replace(key, set, nset)
//            if (suc) Some(tb)
//            else unregisterTable(key, table)
//          }
//          case _ => None
//        }
//      }
//      case _ => None
//    }
//  }
//
//  private def unregisterTables(key: String): Set[String] = {
//    tables.remove(key) match {
//      case Some(set) => set
//      case _ => Set[String]()
//    }
//  }

  private def dropTempTable(sqlContext: SQLContext, table: String): Unit = {
    try {
      sqlContext.dropTempTable(table)
    } catch {
      case e: Throwable => warn(s"drop temp table ${table} fails")
    }
  }

  // -----

  def registerRunGlobalTable(df: DataFrame, table: String): Unit = {
    registerRunTempTable(df, _global, table)
  }

  def registerRunTempTable(df: DataFrame, key: String, table: String): Unit = {
    runTableRegs.registerTable(key, table)
    df.registerTempTable(table)
  }

  def registerCompileGlobalTable(table: String): Unit = {
    registerCompileTempTable(_global, table)
  }

  def registerCompileTempTable(key: String, table: String): Unit = {
    compileTableRegs.registerTable(key, table)
  }

  def unregisterRunTempTable(sqlContext: SQLContext, key: String, table: String): Unit = {
    runTableRegs.unregisterTable(key, table).foreach(dropTempTable(sqlContext, _))
  }

  def unregisterCompileTempTable(key: String, table: String): Unit = {
    compileTableRegs.unregisterTable(key, table)
  }

  def unregisterRunGlobalTables(sqlContext: SQLContext): Unit = {
    unregisterRunTempTables(sqlContext, _global)
  }

  def unregisterCompileGlobalTables(): Unit = {
    unregisterCompileTempTables(_global)
  }

  def unregisterRunTempTables(sqlContext: SQLContext, key: String): Unit = {
    runTableRegs.unregisterTables(key).foreach(dropTempTable(sqlContext, _))
  }

  def unregisterCompileTempTables(key: String): Unit = {
    compileTableRegs.unregisterTables(key)
  }

  def existRunGlobalTable(table: String): Boolean = {
    existRunTempTable(_global, table)
  }

  def existCompileGlobalTable(table: String): Boolean = {
    existCompileTempTable(_global, table)
  }

  def existRunTempTable(key: String, table: String): Boolean = {
    runTableRegs.existTable(key, table)
  }

  def existCompileTempTable(key: String, table: String): Boolean = {
    compileTableRegs.existTable(key, table)
  }

  def getRunGlobalTables(): Set[String] = {
    getRunTempTables(_global)
  }

  def getRunTempTables(key: String): Set[String] = {
    runTableRegs.getTables(key)
  }

}

//object TempKeys {
//  def key(t: Long): String = s"${t}"
//  def key(head: String, t: Long): String = s"${head}_${t}"
//}