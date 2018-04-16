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

import org.apache.griffin.measure.config.params.user.DataSourceParam
import org.apache.griffin.measure.data.source.DataSource
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.ProcessType
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan.{TimeInfo, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait DqEngine extends Loggable with Serializable {

  def runRuleStep(timeInfo: TimeInfo, ruleStep: RuleStep): Boolean

  protected def collectable(): Boolean = false

  def collectMetrics(metricExport: MetricExport): Map[Long, Map[String, Any]]

  //  def collectRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Option[RDD[(Long, Iterable[String])]]
  //
  //  def collectUpdateCacheDatas(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Option[RDD[(Long, Iterable[String])]]

//  def collectUpdateRDD(ruleStep: RuleStep): Option[DataFrame]
//  def collectRecords(timeInfo: TimeInfo, recordExport: RecordExport): Map[Long, DataFrame]


  def collectBatchRecords(recordExport: RecordExport): Option[RDD[String]]
  def collectStreamingRecords(recordExport: RecordExport): (Option[RDD[(Long, Iterable[String])]], Set[Long])

  def collectUpdateDf(dsUpdate: DsUpdate): Option[DataFrame]
}