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

import scala.collection.concurrent.{Map => ConcMap, TrieMap}

object TempTables extends Loggable {

  val tables: ConcMap[Long, Set[String]] = TrieMap[Long, Set[String]]()

  def registerTable(t: Long, table: String): Unit = {
    val set = tables.get(t) match {
      case Some(s) => s + table
      case _ => Set[String](table)
    }
    tables.replace(t, set)
  }

  def unregisterTable(t: Long, table: String): Unit = {
    tables.get(t).foreach { set =>
      val nset = set - table
      tables.replace(t, nset)
    }
  }

  def unregisterTables(t: Long): Unit = {
    tables.remove(t)
  }

  def existTable(t: Long, table: String): Boolean = {
    tables.get(t) match {
      case Some(set) => set.exists(_ == table)
      case _ => false
    }
  }

}
