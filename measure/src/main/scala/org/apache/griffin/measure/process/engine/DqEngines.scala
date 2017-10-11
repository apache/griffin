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
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.step._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class DqEngines(engines: Seq[DqEngine]) extends DqEngine {

  val persistOrder: List[PersistType] = List(MetricPersistType, RecordPersistType)

  def loadData(dataSources: Seq[DataSource], ms: Long): Unit = {
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

//  def persistAllRecords(ruleSteps: Seq[ConcreteRuleStep], persistFactory: PersistFactory,
//                        timeGroups: Iterable[Long]): Unit = {
//    val recordSteps = ruleSteps.filter(_.persistType == RecordPersistType)
//    recordSteps.foreach { step =>
//      collectRecords(step, timeGroups) match {
//        case Some(rdd) => {
//          val name = step.name
//          rdd.foreach { pair =>
//            val (t, items) = pair
//            val persist = persistFactory.getPersists(t)
//            persist.persistRecords(items, name)
//          }
//        }
//        case _ => {
//          println(s"empty records to persist")
//        }
//      }
//    }
//  }
//
//  def updateDataSources(ruleSteps: Seq[ConcreteRuleStep], dataSources: Seq[DataSource],
//                        timeGroups: Iterable[Long]): Unit = {
//    val updateSteps = ruleSteps.filter(_.updateDataSource.nonEmpty)
//    updateSteps.foreach { step =>
//      collectUpdateCacheDatas(step, timeGroups) match {
//        case Some(rdd) => {
//          val udpateDataSources = dataSources.filter { ds =>
//            step.updateDataSource match {
//              case Some(dsName) if (dsName == ds.name) => true
//              case _ => false
//            }
//          }
//          if (udpateDataSources.size > 0) {
//            val name = step.name
//            rdd.foreach { pair =>
//              val (t, items) = pair
//              udpateDataSources.foreach { ds =>
//                ds.dataSourceCacheOpt.foreach(_.updateData(items, t))
//              }
//            }
//          }
//        }
//        case _ => {
//          println(s"empty data source to update")
//        }
//      }
//    }
//  }

  ///////////////////////////

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.runRuleStep(ruleStep)
    }
    if (!ret) warn(s"run rule step warn: no dq engine support ${ruleStep}")
    ret
  }

  ///////////////////////////

//  def collectRecords(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Option[RDD[(Long, Iterable[String])]] = {
//    engines.flatMap { engine =>
//      engine.collectRecords(ruleStep, timeGroups)
//    }.headOption
//  }
//  def collectUpdateCacheDatas(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]): Option[RDD[(Long, Iterable[String])]] = {
//    engines.flatMap { engine =>
//      engine.collectUpdateCacheDatas(ruleStep, timeGroups)
//    }.headOption
//  }
  def collectMetrics(ruleStep: ConcreteRuleStep): Map[Long, Map[String, Any]] = {
    val ret = engines.foldLeft(Map[Long, Map[String, Any]]()) { (ret, engine) =>
      ret ++ engine.collectMetrics(ruleStep)
    }
//    if (ret.isEmpty) warn(s"collect metrics warn: no metrics collected for ${ruleStep}")
    ret
  }

  def collectUpdateRDD(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]
                      ): Option[RDD[(Long, Iterable[String])]] = {
    engines.flatMap { engine =>
      engine.collectUpdateRDD(ruleStep, timeGroups)
    }.headOption
  }

  ////////////////////////////

  def collectUpdateRDDs(ruleSteps: Seq[ConcreteRuleStep], timeGroups: Iterable[Long]
                       ): Seq[(ConcreteRuleStep, RDD[(Long, Iterable[String])])] = {
    ruleSteps.flatMap { rs =>
      collectUpdateRDD(rs, timeGroups) match {
        case Some(rdd) => Some((rs, rdd))
        case _ => None
      }
    }
  }

  def persistAllRecords(stepRdds: Seq[(ConcreteRuleStep, RDD[(Long, Iterable[String])])],
                        persistFactory: PersistFactory): Unit = {
    stepRdds.foreach { stepRdd =>
      val (step, rdd) = stepRdd
      if (step.persistType == RecordPersistType) {
        val name = step.name
        rdd.foreach { pair =>
          val (t, items) = pair
          val persist = persistFactory.getPersists(t)
          persist.persistRecords(items, name)
        }
      }
    }
  }

  def updateDataSources(stepRdds: Seq[(ConcreteRuleStep, RDD[(Long, Iterable[String])])],
                        dataSources: Seq[DataSource]): Unit = {
    stepRdds.foreach { stepRdd =>
      val (step, rdd) = stepRdd
      if (step.updateDataSource.nonEmpty) {
        val udpateDataSources = dataSources.filter { ds =>
          step.updateDataSource match {
            case Some(dsName) if (dsName == ds.name) => true
            case _ => false
          }
        }
        if (udpateDataSources.size > 0) {
          val name = step.name
          rdd.foreach { pair =>
            val (t, items) = pair
            udpateDataSources.foreach { ds =>
              ds.dataSourceCacheOpt.foreach(_.updateData(items, t))
            }
          }
        }
      }
    }
  }

}
