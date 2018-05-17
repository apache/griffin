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
import org.apache.griffin.measure.persist._
import org.apache.griffin.measure.process.temp.TimeRange
import org.apache.griffin.measure.process._
import org.apache.griffin.measure.rule.adaptor.InternalColumns
import org.apache.griffin.measure.rule.dsl._
import org.apache.griffin.measure.rule.plan.{DsUpdate, _}
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

//import scala.concurrent._
//import scala.concurrent.duration.Duration
//import scala.util.{Failure, Success, Try}
//import ExecutionContext.Implicits.global

case class DqEngines(engines: Seq[DqEngine]) extends DqEngine {

  val persistOrder: List[PersistType] = List(MetricPersistType, RecordPersistType)

  def loadData(dataSources: Seq[DataSource], timeInfo: TimeInfo): Map[String, TimeRange] = {
    dataSources.map { ds =>
      (ds.name, ds.loadData(timeInfo))
    }.toMap
  }

  def runRuleSteps(timeInfo: TimeInfo, ruleSteps: Seq[RuleStep]): Unit = {
    ruleSteps.foreach { ruleStep =>
      runRuleStep(timeInfo, ruleStep)
    }
  }

  def persistAllMetrics(metricExports: Seq[MetricExport], persistFactory: PersistFactory
                       ): Unit = {
    val allMetrics: Map[Long, Map[String, Any]] = {
      metricExports.foldLeft(Map[Long, Map[String, Any]]()) { (ret, metricExport) =>
        val metrics = collectMetrics(metricExport)
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

//  private def persistCollectedRecords(recordExport: RecordExport, records: Map[Long, DataFrame],
//                                      persistFactory: PersistFactory, dataSources: Seq[DataSource]): Unit = {
//    val pc = ParallelCounter(records.size)
//    val pro = promise[Boolean]
//    if (records.size > 0) {
//      records.foreach { pair =>
//        val (tmst, df) = pair
//        val persist = persistFactory.getPersists(tmst)
//        val updateDsCaches = recordExport.dataSourceCacheOpt match {
//          case Some(dsName) => dataSources.filter(_.name == dsName).flatMap(_.dataSourceCacheOpt)
//          case _ => Nil
//        }
//        val future = Future {
//          persist.persistRecords(df, recordExport.name)
////          updateDsCaches.foreach(_.updateData(df, tmst))
//          updateDsCaches.foreach(_.updateData(Some(df)))
//          true
//        }
//        future.onComplete {
//          case Success(v) => {
//            pc.finishOne(v)
//            if (pc.checkDone) pro.trySuccess(pc.checkResult)
//          }
//          case Failure(ex) => {
//            println(s"plan step failure: ${ex.getMessage}")
//            pc.finishOne(false)
//            if (pc.checkDone) pro.trySuccess(pc.checkResult)
//          }
//        }
//      }
//    } else pro.trySuccess(true)
//
//    Await.result(pro.future, Duration.Inf)
//  }

  def persistAllRecords(recordExports: Seq[RecordExport],
                        persistFactory: PersistFactory, dataSources: Seq[DataSource]
                       ): Unit = {
    // method 1: multi thread persist multi data frame
//    recordExports.foreach { recordExport =>
//      val records = collectRecords(timeInfo, recordExport, procType)
//      persistCollectedRecords(recordExport, records, persistFactory, dataSources)
//    }

    // method 2: multi thread persist multi iterable
    recordExports.foreach { recordExport =>
      recordExport.mode match {
        case SimpleMode => {
          collectBatchRecords(recordExport).foreach { rdd =>
            persistCollectedBatchRecords(recordExport, rdd, persistFactory)
          }
        }
        case TimestampMode => {
          val (rddOpt, emptySet) = collectStreamingRecords(recordExport)
          persistCollectedStreamingRecords(recordExport, rddOpt, emptySet, persistFactory, dataSources)
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
  def collectStreamingRecords(recordExport: RecordExport): (Option[RDD[(Long, Iterable[String])]], Set[Long]) = {
    val ret = engines.foldLeft((None: Option[RDD[(Long, Iterable[String])]], Set[Long]())) { (ret, engine) =>
      if (ret._1.nonEmpty || ret._2.nonEmpty) ret else engine.collectStreamingRecords(recordExport)
    }
    ret
  }

  private def persistCollectedBatchRecords(recordExport: RecordExport,
                                           records: RDD[String], persistFactory: PersistFactory
                                          ): Unit = {
    val persist = persistFactory.getPersists(recordExport.defTimestamp)
    persist.persistRecords(records, recordExport.name)
  }

  private def persistCollectedStreamingRecords(recordExport: RecordExport, recordsOpt: Option[RDD[(Long, Iterable[String])]],
                                               emtpyRecordKeys: Set[Long], persistFactory: PersistFactory,
                                               dataSources: Seq[DataSource]
                                              ): Unit = {
//    val updateDsCaches = recordExport.dataSourceCacheOpt match {
//      case Some(dsName) => dataSources.filter(_.name == dsName).flatMap(_.dataSourceCacheOpt)
//      case _ => Nil
//    }

    recordsOpt.foreach { records =>
      records.foreach { pair =>
        val (tmst, strs) = pair
        val persist = persistFactory.getPersists(tmst)

        persist.persistRecords(strs, recordExport.name)
//        updateDsCaches.foreach(_.updateData(strs, tmst))
      }
    }

    emtpyRecordKeys.foreach { t =>
      val persist = persistFactory.getPersists(t)
      persist.persistRecords(Nil, recordExport.name)
//      updateDsCaches.foreach(_.updateData(Nil, t))
    }
  }

  ///////////////////////////

  def runRuleStep(timeInfo: TimeInfo, ruleStep: RuleStep): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.runRuleStep(timeInfo, ruleStep)
    }
    if (!ret) warn(s"run rule step warn: no dq engine support ${ruleStep}")
    ret
  }

  ///////////////////////////

  def collectMetrics(metricExport: MetricExport
                    ): Map[Long, Map[String, Any]] = {
    val ret = engines.foldLeft(Map[Long, Map[String, Any]]()) { (ret, engine) =>
      if (ret.nonEmpty) ret else engine.collectMetrics(metricExport)
    }
    ret
  }

  def collectUpdateDf(dsUpdate: DsUpdate): Option[DataFrame] = {
    val ret = engines.foldLeft(None: Option[DataFrame]) { (ret, engine) =>
      if (ret.nonEmpty) ret else engine.collectUpdateDf(dsUpdate)
    }
    ret
  }

  def updateDataSources(dsUpdates: Seq[DsUpdate],
                        dataSources: Seq[DataSource]): Unit = {
    dsUpdates.foreach { dsUpdate =>
      val dsName = dsUpdate.dsName
      collectUpdateDf(dsUpdate) match {
        case Some(df) => {
          dataSources.find(_.name == dsName).foreach(_.updateData(df))
        }
        case _ => {
          // do nothing
        }
      }
    }
  }

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