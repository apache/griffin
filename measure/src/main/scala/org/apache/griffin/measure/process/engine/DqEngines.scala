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

  def persistAllResults(ruleSteps: Seq[ConcreteRuleStep], persistFactory: PersistFactory): Unit = {
    // 1. group by same persist types
    val groupedRuleSteps = ruleSteps.groupBy(_.persistType)

    // 2. persist results in order [metric, record]
    persistOrder.foreach { prstType =>
      val steps = groupedRuleSteps.get(prstType) match {
        case Some(a) => a
        case _ => Nil
      }
      prstType match {
        case MetricPersistType => {
//          val metrics = steps.foldLeft(Map[String, Any]())(_ ++ collectMetrics(_))
          val metrics: Map[Long, Map[String, Any]] = {
            steps.foldLeft(Map[Long, Map[String, Any]]()) { (ret, step) =>
              val metrics = collectMetrics(step)
              metrics.foldLeft(ret) { (total, pair) =>
                val (k, v) = pair
                ret.get(k) match {
                  case Some(map) => ret + (k -> (map ++ v))
                  case _ => ret + pair
                }
              }
            }
          }
          metrics.foreach { pair =>
            val (t, metric) = pair
            val persist = persistFactory.getPersists(t)
            persist.persistMetrics(metric)
          }
        }
        case RecordPersistType => {
          steps.foreach { ruleStep =>
            persistRecords(ruleStep, persistFactory)
          }
        }
        case _ => {
          warn(s"${prstType} is not persistable")
        }
      }
    }
  }

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

  def persistRecords(ruleStep: ConcreteRuleStep, persistFactory: PersistFactory): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.persistRecords(ruleStep, persistFactory)
    }
    if (!ret) error(s"persist result warn: no dq engine support ${ruleStep}")
    ret
  }
  def collectMetrics(ruleStep: ConcreteRuleStep): Map[Long, Map[String, Any]] = {
    val ret = engines.foldLeft(Map[Long, Map[String, Any]]()) { (ret, engine) =>
      ret ++ engine.collectMetrics(ruleStep)
    }
    if (ret.isEmpty) warn(s"collect metrics warn: no metrics collected for ${ruleStep}")
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
