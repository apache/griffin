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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.griffin.measure.config.params.user.DataSourceParam
import org.apache.griffin.measure.data.source._
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.{Persist, PersistFactory}
import org.apache.griffin.measure.process.{BatchProcessType, ProcessType, StreamingProcessType}
import org.apache.griffin.measure.rule.adaptor.InternalColumns
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan._
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import ExecutionContext.Implicits.global

case class DqEngines(engines: Seq[DqEngine]) extends DqEngine {

  val persistOrder: List[PersistType] = List(MetricPersistType, RecordPersistType)

  def loadData(dataSources: Seq[DataSource], timeInfo: TimeInfo): Map[String, Set[Long]] = {
    dataSources.map { ds =>
      (ds.name, ds.loadData(timeInfo))
    }.toMap
  }

  def runRuleSteps(timeInfo: TimeInfo, ruleSteps: Seq[RuleStep]): Unit = {
    ruleSteps.foreach { ruleStep =>
      runRuleStep(timeInfo, ruleStep)
    }
  }

  def persistAllMetrics(timeInfo: TimeInfo, metricExports: Seq[MetricExport],
                        procType: ProcessType, persistFactory: PersistFactory
                       ): Unit = {
    val allMetrics: Map[Long, Map[String, Any]] = {
      metricExports.foldLeft(Map[Long, Map[String, Any]]()) { (ret, step) =>
        val metrics = collectMetrics(timeInfo, step, procType)
        metrics.foldLeft(ret) { (total, pair) =>
          val (k, v) = pair
          total.get(k) match {
            case Some(map) => total + (k -> (map ++ v))
            case _ => total + pair
          }
        }
      }
    }

    allMetrics.foreach { pair =>
      val (t, metric) = pair
      val persist = persistFactory.getPersists(t)
      persist.persistMetrics(metric)
    }
  }

  private def persistCollectedRecords(recordExport: RecordExport, records: Map[Long, DataFrame],
                                      persistFactory: PersistFactory, dataSources: Seq[DataSource]): Unit = {
    val pc = ParallelCounter(records.size)
    val pro = promise[Boolean]
    if (records.size > 0) {
      records.foreach { pair =>
        val (tmst, df) = pair
        val persist = persistFactory.getPersists(tmst)
        val updateDsCaches = recordExport.dataSourceCacheOpt match {
          case Some(dsName) => dataSources.filter(_.name == dsName).flatMap(_.dataSourceCacheOpt)
          case _ => Nil
        }
        val future = Future {
//        df.cache
          persist.persistRecords(df, recordExport.name)
          updateDsCaches.foreach(_.updateData(df, tmst))
//        df.unpersist
          true
        }
        future.onComplete {
          case Success(v) => {
            pc.finishOne(v)
            if (pc.checkDone) pro.trySuccess(pc.checkResult)
          }
          case Failure(ex) => {
            println(s"plan step failure: ${ex.getMessage}")
            pc.finishOne(false)
            if (pc.checkDone) pro.trySuccess(pc.checkResult)
          }
        }
      }
    } else pro.trySuccess(true)

    Await.result(pro.future, Duration.Inf)
  }

  def persistAllRecords(timeInfo: TimeInfo, recordExports: Seq[RecordExport], procType: ProcessType,
                        persistFactory: PersistFactory, dataSources: Seq[DataSource]
                       ): Unit = {
    // method 1: multi thread persist multi data frame
//    recordExports.foreach { recordExport =>
//      val records = collectRecords(timeInfo, recordExport, procType)
//      persistCollectedRecords(recordExport, records, persistFactory, dataSources)
//    }

    // method 2: multi thread persist multi iterable
    recordExports.foreach { recordExport =>
//      val records = collectRecords(timeInfo, recordExport, procType)
      procType match {
        case BatchProcessType => {
          collectBatchRecords(recordExport).foreach { rdd =>
            persistCollectedBatchRecords(timeInfo, recordExport, rdd, persistFactory)
          }
        }
        case StreamingProcessType => {
          collectStreamingRecords(recordExport).foreach { rddPair =>
            persistCollectedStreamingRecords(recordExport, rddPair._1, rddPair._2, persistFactory, dataSources)
          }
        }
      }
    }
  }

  def collectBatchRecords(recordExport: RecordExport): Option[RDD[String]] = {
    val ret = engines.foldLeft(None: Option[RDD[String]]) { (ret, engine) =>
      if (ret.nonEmpty) ret else engine.collectBatchRecords(recordExport)
    }
    ret
  }
  def collectStreamingRecords(recordExport: RecordExport): Option[(RDD[(Long, Iterable[String])], Set[Long])] = {
    val ret = engines.foldLeft(None: Option[(RDD[(Long, Iterable[String])], Set[Long])]) { (ret, engine) =>
      if (ret.nonEmpty) ret else engine.collectStreamingRecords(recordExport)
    }
    ret
  }

  private def persistCollectedBatchRecords(timeInfo: TimeInfo, recordExport: RecordExport,
                                           records: RDD[String], persistFactory: PersistFactory
                                          ): Unit = {
    val persist = persistFactory.getPersists(timeInfo.calcTime)
    persist.persistRecords(records, recordExport.name)
  }

  private def persistCollectedStreamingRecords(recordExport: RecordExport, records: RDD[(Long, Iterable[String])],
                                               emtpyRecordKeys: Set[Long], persistFactory: PersistFactory,
                                               dataSources: Seq[DataSource]
                                              ): Unit = {
    val updateDsCaches = recordExport.dataSourceCacheOpt match {
      case Some(dsName) => dataSources.filter(_.name == dsName).flatMap(_.dataSourceCacheOpt)
      case _ => Nil
    }

    records.foreach { pair =>
      val (tmst, strs) = pair
      val persist = persistFactory.getPersists(tmst)

      persist.persistRecords(strs, recordExport.name)
      updateDsCaches.foreach(_.updateData(strs, tmst))
    }

    emtpyRecordKeys.foreach { t =>
      val persist = persistFactory.getPersists(t)
      persist.persistRecords(Nil, recordExport.name)
      updateDsCaches.foreach(_.updateData(Nil, t))
    }
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

  def runRuleStep(timeInfo: TimeInfo, ruleStep: RuleStep): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.runRuleStep(timeInfo, ruleStep)
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
  def collectMetrics(timeInfo: TimeInfo, metricExport: MetricExport, procType: ProcessType
                    ): Map[Long, Map[String, Any]] = {
    val ret = engines.foldLeft(Map[Long, Map[String, Any]]()) { (ret, engine) =>
      if (ret.nonEmpty) ret else engine.collectMetrics(timeInfo, metricExport, procType)
    }
    ret
  }

  def collectRecords(timeInfo: TimeInfo, recordExport: RecordExport, procType: ProcessType
                    ): Map[Long, DataFrame] = {
    val ret = engines.foldLeft(Map[Long, DataFrame]()) { (ret, engine) =>
      if (ret.nonEmpty) ret else engine.collectRecords(timeInfo, recordExport, procType)
    }
    ret
  }

  def collectUpdateRDD(ruleStep: RuleStep): Option[DataFrame] = {
//    engines.flatMap { engine =>
//      engine.collectUpdateRDD(ruleStep)
//    }.headOption
    None
  }

//  def collectUpdateRDD(ruleStep: ConcreteRuleStep, timeGroups: Iterable[Long]
//                      ): Option[RDD[(Long, Iterable[String])]] = {
//    engines.flatMap { engine =>
//      engine.collectUpdateRDD(ruleStep, timeGroups)
//    }.headOption
//  }

  ////////////////////////////

  def collectUpdateRDDs(ruleSteps: Seq[RuleStep], timeGroups: Set[Long]
                       ): Seq[(RuleStep, DataFrame)] = {
//    ruleSteps.flatMap { rs =>
//      val t = rs.timeInfo.tmst
//      if (timeGroups.contains(t)) {
//        collectUpdateRDD(rs).map((rs, _))
//      } else None
//    }
    Nil
  }

//  def collectUpdateRDDs(ruleSteps: Seq[ConcreteRuleStep], timeGroups: Iterable[Long]
//                       ): Seq[(ConcreteRuleStep, RDD[(Long, Iterable[String])])] = {
//    ruleSteps.flatMap { rs =>
//      collectUpdateRDD(rs, timeGroups) match {
//        case Some(rdd) => Some((rs, rdd))
//        case _ => None
//      }
//    }
//  }

  def persistAllRecords(stepRdds: Seq[(RuleStep, DataFrame)],
                        persistFactory: PersistFactory): Unit = {
//    stepRdds.foreach { stepRdd =>
//      val (step, df) = stepRdd
//      if (step.ruleInfo.persistType == RecordPersistType) {
//        val name = step.ruleInfo.name
//        val t = step.timeInfo.tmst
//        val persist = persistFactory.getPersists(t)
//        persist.persistRecords(df, name)
//      }
//    }
  }

//  def persistAllRecords(stepRdds: Seq[(ConcreteRuleStep, RDD[(Long, Iterable[String])])],
//                        persistFactory: PersistFactory): Unit = {
//    stepRdds.foreach { stepRdd =>
//      val (step, rdd) = stepRdd
//      if (step.ruleInfo.persistType == RecordPersistType) {
//        val name = step.name
//        rdd.foreach { pair =>
//          val (t, items) = pair
//          val persist = persistFactory.getPersists(t)
//          persist.persistRecords(items, name)
//        }
//      }
//    }
//  }

  def updateDataSources(stepRdds: Seq[(RuleStep, DataFrame)],
                        dataSources: Seq[DataSource]): Unit = {
//    stepRdds.foreach { stepRdd =>
//      val (step, df) = stepRdd
//      if (step.ruleInfo.cacheDataSourceOpt.nonEmpty) {
//        val udpateDsCaches = dataSources.filter { ds =>
//          step.ruleInfo.cacheDataSourceOpt match {
//            case Some(dsName) if (dsName == ds.name) => true
//            case _ => false
//          }
//        }.flatMap(_.dataSourceCacheOpt)
//        if (udpateDsCaches.size > 0) {
//          val t = step.timeInfo.tmst
//          udpateDsCaches.foreach(_.updateData(df, t))
//        }
//      }
//    }
  }

//  def updateDataSources(stepRdds: Seq[(ConcreteRuleStep, RDD[(Long, Iterable[String])])],
//                        dataSources: Seq[DataSource]): Unit = {
//    stepRdds.foreach { stepRdd =>
//      val (step, rdd) = stepRdd
//      if (step.ruleInfo.cacheDataSourceOpt.nonEmpty) {
//        val udpateDataSources = dataSources.filter { ds =>
//          step.ruleInfo.cacheDataSourceOpt match {
//            case Some(dsName) if (dsName == ds.name) => true
//            case _ => false
//          }
//        }
//        if (udpateDataSources.size > 0) {
//          val name = step.name
//          rdd.foreach { pair =>
//            val (t, items) = pair
//            udpateDataSources.foreach { ds =>
//              ds.dataSourceCacheOpt.foreach(_.updateData(items, t))
//            }
//          }
//        }
//      }
//    }
//  }

}

case class ParallelCounter(total: Int) extends Serializable {
  private val done: AtomicInteger = new AtomicInteger(0)
  private val result: AtomicInteger = new AtomicInteger(0)
  def finishOne(suc: Boolean): Unit = {
    if (suc) result.incrementAndGet
    done.incrementAndGet
  }
  def checkDone: Boolean = {
    done.get() >= total
  }
  def checkResult: Boolean = {
    if (total > 0) result.get() > 0 else true
  }
}