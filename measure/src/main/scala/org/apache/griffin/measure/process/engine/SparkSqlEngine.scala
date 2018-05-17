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
package org.apache.griffin.measure.process.engine

import java.util.Date

import org.apache.griffin.measure.config.params.user.DataSourceParam
import org.apache.griffin.measure.data.source._
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.temp.{DataFrameCaches, TableRegisters}
import org.apache.griffin.measure.rule.adaptor.{GlobalKeys, InternalColumns}
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext

case class SparkSqlEngine(sqlContext: SQLContext) extends SparkDqEngine {

  override protected def collectable(): Boolean = true

  def runRuleStep(timeInfo: TimeInfo, ruleStep: RuleStep): Boolean = {
    ruleStep match {
      case rs @ SparkSqlStep(name, rule, details, _, _) => {
        try {
          val rdf = if (rs.isGlobal && !TableRegisters.existRunGlobalTable(name)) {
            details.get(GlobalKeys._initRule) match {
              case Some(initRule: String) => sqlContext.sql(initRule)
              case _ => sqlContext.emptyDataFrame
            }
          } else sqlContext.sql(rule)

//          println(name)
//          rdf.show(3)

          if (rs.isGlobal) {
            if (rs.needCache) DataFrameCaches.cacheGlobalDataFrame(name, rdf)
            TableRegisters.registerRunGlobalTable(rdf, name)
          } else {
            if (rs.needCache) DataFrameCaches.cacheDataFrame(timeInfo.key, name, rdf)
            TableRegisters.registerRunTempTable(rdf, timeInfo.key, name)
          }
          true
        } catch {
          case e: Throwable => {
            error(s"run spark sql [ ${rule} ] error: ${e.getMessage}")
            false
          }
        }
      }
      case _ => false
    }
  }

}




