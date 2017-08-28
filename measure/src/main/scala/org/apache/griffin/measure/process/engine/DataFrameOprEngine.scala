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
import org.apache.griffin.measure.data.source.{DataSource, DataSourceFactory}
import org.apache.griffin.measure.persist.Persist
import org.apache.griffin.measure.rules.step._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext

case class DataFrameOprEngine(sqlContext: SQLContext, @transient ssc: StreamingContext
                             ) extends DqEngine {

  def genDataSource(dataSourceParam: DataSourceParam): Option[DataSource] = {
    DataSourceFactory.genDataSource(sqlContext, ssc, dataSourceParam)
  }

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    ruleStep match {
      case DfOprStep(name, rule, details) => {
        try {
          rule match {
            case DataFrameOprs._fromJson => {
              val df = DataFrameOprs.fromJson(sqlContext, name, details)
              df.registerTempTable(name)
            }
            case _ => {
              throw new Exception(s"df opr [ ${rule} ] not supported")
            }
          }
          true
        } catch {
          case e: Throwable => {
            error(s"run df opr [ ${rule} ] error: ${e.getMessage}")
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
      case DfOprStep(name, _, _) => {
        try {
          val nonLog = s"[ ${name} ] not persisted"
          persist.log(curTime, nonLog)

          true
        } catch {
          case e: Throwable => {
            error(s"persist result ${ruleStep.name} error: ${e.getMessage}")
            false
          }
        }
      }
      case _ => false
    }
  }

}

object DataFrameOprs {

  final val _fromJson = "from_json"

  def fromJson(sqlContext: SQLContext, name: String, details: Map[String, Any]): DataFrame = {
    val _dfName = "df.name"
    val _colName = "col.name"
    val dfName = details.getOrElse(_dfName, name).toString
    val colNameOpt = details.get(_colName).map(_.toString)

    val df = sqlContext.table(dfName)
    val rdd = colNameOpt match {
      case Some(colName: String) => df.map(_.getAs[String](colName))
      case _ => df.map(_.getAs[String](0))
    }
    sqlContext.read.json(rdd)
  }

}



