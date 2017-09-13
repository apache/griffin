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
import org.apache.griffin.measure.data.connector.GroupByColumn
import org.apache.griffin.measure.data.source._
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.rules.dsl._
import org.apache.griffin.measure.rules.step._
import org.apache.spark.rdd.RDD

case class DqEngines(engines: Seq[DqEngine]) extends DqEngine {

  val persistOrder: List[PersistType] = List(MetricPersistType, RecordPersistType)

  def loadData(dataSources: Seq[DataSource], ms: Long): Unit = {
//    val dataSources = dataSourceParams.flatMap { param =>
//      genDataSource(param)
//    }
    dataSources.foreach { ds =>
      ds.loadData(ms)
    }
  }

  def runRuleSteps(ruleSteps: Seq[ConcreteRuleStep]): Unit = {
    ruleSteps.foreach { ruleStep =>
      runRuleStep(ruleStep)
    }
  }

  def persistAllMetrics(ruleSteps: Seq[ConcreteRuleStep], persistFactory: PersistFactory
                       ): Iterable[Long] = {
    val metricSteps = ruleSteps.filter(_.persistType == MetricPersistType)
    val allMetrics: Map[Long, Map[String, Any]] = {
      metricSteps.foldLeft(Map[Long, Map[String, Any]]()) { (ret, step) =>
        val metrics = collectMetrics(step)
        metrics.foldLeft(ret) { (total, pair) =>
          val (k, v) = pair
          total.get(k) match {
            case Some(map) => total + (k -> (map ++ v))
            case _ => total + pair
          }
        }
      }
    }
    val updateTimeGroups = allMetrics.keys
    allMetrics.foreach { pair =>
      val (t, metric) = pair
      val persist = persistFactory.getPersists(t)
      persist.persistMetrics(metric)
    }
    updateTimeGroups
  }

  def persistAllRecords(ruleSteps: Seq[ConcreteRuleStep], persistFactory: PersistFactory,
                        timeGroups: Iterable[Long]): Unit = {
    val recordSteps = ruleSteps.filter(_.persistType == RecordPersistType)
    recordSteps.foreach { step =>
      val name = step.name
      val records = collectRecords(step, timeGroups)
      records.foreach { pair =>
        val (t, recs) = pair
        val persist = persistFactory.getPersists(t)
        persist.persistRecords(recs, name)
      }
    }
  }

  def updateDataSources(ruleSteps: Seq[ConcreteRuleStep], dataSources: Seq[DataSource],
                        timeGroups: Iterable[Long]): Unit = {
    // fixme
//    val recordSteps = ruleSteps.filter(_.persistType == RecordPersistType)
//    recordSteps.foreach { step =>
//      val name = step.name
//      val records = collectRecords(step, timeGroups)
//      records.foreach { pair =>
//        val (t, recs) = pair
//        dataSources
//        val persist = persistFactory.getPersists(t)
//        persist.persistRecords(recs, name)
//      }
//    }
  }

//  def persistAllResults(ruleSteps: Seq[ConcreteRuleStep], persistFactory: PersistFactory): Unit = {
//    // 1. persist metric
//    val metricSteps = ruleSteps.filter(_.persistType == MetricPersistType)
//    val allMetrics: Map[Long, Map[String, Any]] = {
//      metricSteps.foldLeft(Map[Long, Map[String, Any]]()) { (ret, step) =>
//        val metrics = collectMetrics(step)
//        metrics.foldLeft(ret) { (total, pair) =>
//          val (k, v) = pair
//          total.get(k) match {
//            case Some(map) => total + (k -> (map ++ v))
//            case _ => total + pair
//          }
//        }
//      }
//    }
//    val updateTimeGroups = allMetrics.keys
//    allMetrics.foreach { pair =>
//      val (t, metric) = pair
//      val persist = persistFactory.getPersists(t)
//      persist.persistMetrics(metric)
//    }
//
//    // 2. persist record
//    val recordSteps = ruleSteps.filter(_.persistType == RecordPersistType)
//    recordSteps.foreach { step =>
//      val name = step.name
//      val records = collectRecords(step, updateTimeGroups)
//      records.foreach { pair =>
//        val (t, recs) = pair
//        val persist = persistFactory.getPersists(t)
//        persist.persistRecords(recs, name)
//      }
//    }
//  }

//  def genDataSource(dataSourceParam: DataSourceParam): Option[DirectDataSource] = {
//    val ret = engines.foldLeft(None: Option[DirectDataSource]) { (dsOpt, engine) =>
//      if (dsOpt.isEmpty) engine.genDataSource(dataSourceParam) else dsOpt
//    }
//    if (ret.isEmpty) warn(s"init data source warn: no dq engine support ${dataSourceParam}")
//    ret
//  }

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.runRuleStep(ruleStep)
    }
    if (!ret) warn(s"run rule step warn: no dq engine support ${ruleStep}")
    ret
  }

//  def persistRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long], persistFactory: PersistFactory): Boolean = {
//    val ret = engines.foldLeft(false) { (done, engine) =>
//      done || engine.persistRecords(ruleStep, timeGroups, persistFactory)
//    }
//    if (!ret) error(s"persist result warn: no dq engine support ${ruleStep}")
//    ret
//  }
  def collectRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Map[Long, RDD[String]] = {
    val ret = engines.foldLeft(Map[Long, RDD[String]]()) { (ret, engine) =>
      ret ++ engine.collectRecords(ruleStep, timeGroups)
    }
//    if (ret.isEmpty) warn(s"collect records warn: no records collected for ${ruleStep}")
    ret
  }
  def collectMetrics(ruleStep: ConcreteRuleStep): Map[Long, Map[String, Any]] = {
    val ret = engines.foldLeft(Map[Long, Map[String, Any]]()) { (ret, engine) =>
      ret ++ engine.collectMetrics(ruleStep)
    }
//    if (ret.isEmpty) warn(s"collect metrics warn: no metrics collected for ${ruleStep}")
    ret
//    val ret = engines.foldLeft(Map[Long, Map[String, Any]]()) { (ret, engine) =>
//      val metrics: Map[Long, Map[String, Any]] = engine.collectMetrics(ruleStep)
//      metrics.foldLeft(ret) { (total, pair) =>
//        val (k, v) = pair
//        ret.get(k) match {
//          case Some(map) => ret + (k -> (map ++ v))
//          case _ => ret + pair
//        }
//      }
//    }
//    if (ret.isEmpty) error(s"collect metrics warn: no metrics collected for ${ruleStep}")
//    ret
  }

//  def persistResults(ruleSteps: Seq[ConcreteRuleStep], persist: Persist, persistType: PersistType): Boolean = {
//    val ret = engines.foldLeft(false) { (done, engine) =>
//      done || engine.persistResults(ruleSteps, persist, persistType)
//    }
//    if (!ret) error(s"persist result warn: no dq engine support ${ruleSteps}")
//    ret
//  }

}
