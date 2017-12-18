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

object TempTables extends Loggable {

  val tables: ConcMap[String, Set[String]] = TrieMap[String, Set[String]]()

  private def registerTable(key: String, table: String): Unit = {
    tables.get(key) match {
      case Some(set) => {
        val suc = tables.replace(key, set, set + table)
        if (!suc) registerTable(key, table)
      }
      case _ => {
        val oldOpt = tables.putIfAbsent(key, Set[String](table))
        if (oldOpt.nonEmpty) registerTable(key, table)
      }
    }
  }

  private def unregisterTable(key: String, table: String): Option[String] = {
    tables.get(key) match {
      case Some(set) => {
        val ftb = set.find(_ == table)
        ftb match {
          case Some(tb) => {
            val nset = set - tb
            val suc = tables.replace(key, set, nset)
            if (suc) Some(tb)
            else unregisterTable(key, table)
          }
          case _ => None
        }
      }
      case _ => None
    }
  }

  private def unregisterTables(key: String): Set[String] = {
    tables.remove(key) match {
      case Some(set) => set
      case _ => Set[String]()
    }
  }

  private def dropTempTable(sqlContext: SQLContext, table: String): Unit = {
    try {
      sqlContext.dropTempTable(table)
    } catch {
      case e: Throwable => warn(s"drop temp table ${table} fails")
    }
  }

  // -----

  def registerTempTable(df: DataFrame, key: String, table: String): Unit = {
    registerTable(key, table)
    df.registerTempTable(table)
  }

  def registerTempTableNameOnly(key: String, table: String): Unit = {
    registerTable(key, table)
  }

  def unregisterTempTable(sqlContext: SQLContext, key: String, table: String): Unit = {
    unregisterTable(key, table).foreach(dropTempTable(sqlContext, _))
  }

  def unregisterTempTables(sqlContext: SQLContext, key: String): Unit = {
    unregisterTables(key).foreach(dropTempTable(sqlContext, _))
  }

  def existTable(key: String, table: String): Boolean = {
    tables.get(key) match {
      case Some(set) => set.exists(_ == table)
      case _ => false
    }
  }

}

object TempKeys {
  def key(t: Long): String = s"${t}"
  def key(head: String, t: Long): String = s"${head}_${t}"
}