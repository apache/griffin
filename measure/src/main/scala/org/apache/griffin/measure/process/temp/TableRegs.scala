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

import org.apache.spark.sql.SQLContext

import scala.collection.concurrent.{TrieMap, Map => ConcMap}

case class TableRegs() {

  private val tables: ConcMap[String, Set[String]] = TrieMap[String, Set[String]]()

  def registerTable(key: String, table: String): Unit = {
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

  def unregisterTable(key: String, table: String): Option[String] = {
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

  def unregisterTables(key: String): Set[String] = {
    tables.remove(key) match {
      case Some(set) => set
      case _ => Set[String]()
    }
  }

  def existTable(key: String, table: String): Boolean = {
    tables.get(key) match {
      case Some(set) => set.contains(table)
      case _ => false
    }
  }

  def getTables(key: String): Set[String] = {
    tables.get(key) match {
      case Some(set) => set
      case _ => Set[String]()
    }
  }

}
