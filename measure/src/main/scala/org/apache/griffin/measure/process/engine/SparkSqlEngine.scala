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
import org.apache.griffin.measure.persist.Persist
import org.apache.griffin.measure.rules.dsl.{MetricPersistType, RecordPersistType}
import org.apache.griffin.measure.rules.step._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext

case class SparkSqlEngine(sqlContext: SQLContext, @transient ssc: StreamingContext
                         ) extends DqEngine {

  def genDataSource(dataSourceParam: DataSourceParam): Option[DataSource] = {
    DataSourceFactory.genDataSource(sqlContext, ssc, dataSourceParam)
  }

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    ruleStep match {
      case SparkSqlStep(name, rule, _) => {
        try {
          val rdf = sqlContext.sql(rule)
          rdf.registerTempTable(name)
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

  def persistResult(ruleStep: ConcreteRuleStep, persist: Persist): Boolean = {
    val curTime = new Date().getTime
    ruleStep match {
      case SparkSqlStep(name, _, persistType) => {
        try {
          persistType match {
            case RecordPersistType => {
              val pdf = sqlContext.table(name)
              val recordRdd = pdf.toJSON

              persist.records(recordRdd, name)

              val recordLog = s"[ ${name} ] persist records"
              persist.log(curTime, recordLog)
            }
            case MetricPersistType => {
              val pdf = sqlContext.table(name)
              val recordRdd = pdf.toJSON

              val metric = recordRdd.collect
              persist.records(metric, name)

              val metricLog = s"[ ${name} ] persist metric \n${metric.mkString("\n")}"
              persist.log(curTime, metricLog)
            }
            case _ => {
              val nonLog = s"[ ${name} ] not persisted"
              persist.log(curTime, nonLog)
            }
          }

          true
        } catch {
          case e: Throwable => {
            error(s"persist result ${name} error: ${e.getMessage}")
            false
          }
        }
      }
      case _ => false
    }
  }

}




