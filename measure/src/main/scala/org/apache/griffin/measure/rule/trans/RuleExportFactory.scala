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
package org.apache.griffin.measure.rule.trans

import org.apache.griffin.measure.process.ExportMode
import org.apache.griffin.measure.rule.dsl.CollectType
import org.apache.griffin.measure.rule.plan._

import org.apache.griffin.measure.utils.ParamUtil._

object RuleExportFactory {

  def genMetricExport(param: Map[String, Any], name: String, stepName: String,
                      defTimestamp: Long, mode: ExportMode
                     ): MetricExport = {
    MetricExport(
      ExportParamKeys.getName(param, name),
      stepName,
      ExportParamKeys.getCollectType(param),
      defTimestamp,
      mode
    )
  }
  def genRecordExport(param: Map[String, Any], name: String, stepName: String,
                      defTimestamp: Long, mode: ExportMode
                     ): RecordExport = {
    RecordExport(
      ExportParamKeys.getName(param, name),
      stepName,
      ExportParamKeys.getDataSourceCacheOpt(param),
      ExportParamKeys.getOriginDFOpt(param),
      defTimestamp,
      mode
    )
  }

}

object ExportParamKeys {
  val _name = "name"
  val _collectType = "collect.type"
  val _dataSourceCache = "data.source.cache"
  val _originDF = "origin.DF"

  def getName(param: Map[String, Any], defName: String): String = param.getString(_name, defName)
  def getCollectType(param: Map[String, Any]): CollectType = CollectType(param.getString(_collectType, ""))
  def getDataSourceCacheOpt(param: Map[String, Any]): Option[String] = param.get(_dataSourceCache).map(_.toString)
  def getOriginDFOpt(param: Map[String, Any]): Option[String] = param.get(_originDF).map(_.toString)
}