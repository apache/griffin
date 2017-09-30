///*
//Licensed to the Apache Software Foundation (ASF) under one
//or more contributor license agreements.  See the NOTICE file
//distributed with this work for additional information
//regarding copyright ownership.  The ASF licenses this file
//to you under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.
//*/
//package org.apache.griffin.measure.data.connector.direct
//
//import org.apache.griffin.measure.config.params.user.DataConnectorParam
//import org.apache.griffin.measure.data.connector.DataConnectorFactory
//import org.apache.griffin.measure.data.connector.cache.CacheDataConnector
//import org.apache.griffin.measure.data.connector.streaming.StreamingDataConnector
//import org.apache.griffin.measure.result._
//import org.apache.griffin.measure.rule._
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.StreamingContext
//
//import scala.util.{Failure, Success, Try}
//
//case class KafkaCacheDirectDataConnector(@transient streamingDataConnectorTry: Try[StreamingDataConnector],
//                                         cacheDataConnectorTry: Try[CacheDataConnector],
//                                         dataConnectorParam: DataConnectorParam,
//                                         ruleExprs: RuleExprs,
//                                         constFinalExprValueMap: Map[String, Any]
//                                        ) extends StreamingCacheDirectDataConnector {
//
//  val cacheDataConnector: CacheDataConnector = cacheDataConnectorTry match {
//    case Success(cntr) => cntr
//    case Failure(ex) => throw ex
//  }
//  @transient val streamingDataConnector: StreamingDataConnector = streamingDataConnectorTry match {
//    case Success(cntr) => cntr
//    case Failure(ex) => throw ex
//  }
//
//  protected def transform(rdd: RDD[(streamingDataConnector.K, streamingDataConnector.V)],
//                          ms: Long
//                         ): RDD[Map[String, Any]] = {
//    val dataInfoMap = DataInfo.cacheInfoList.map(_.defWrap).toMap + TimeStampInfo.wrap(ms)
//
//    rdd.flatMap { kv =>
//      val msg = kv._2
//
//      val cacheExprValueMaps = ExprValueUtil.genExprValueMaps(Some(msg), ruleExprs.cacheExprs, constFinalExprValueMap)
//      val finalExprValueMaps = ExprValueUtil.updateExprValueMaps(ruleExprs.finalCacheExprs, cacheExprValueMaps)
//
//      finalExprValueMaps.map { vm =>
//        vm ++ dataInfoMap
//      }
//    }
//  }
//
//  def metaData(): Try[Iterable[(String, String)]] = Try {
//    Map.empty[String, String]
//  }
//
//  def data(): Try[RDD[(Product, (Map[String, Any], Map[String, Any]))]] = Try {
//    cacheDataConnector.readData match {
//      case Success(rdd) => {
//        rdd.flatMap { row =>
//          val finalExprValueMap = ruleExprs.finalCacheExprs.flatMap { expr =>
//            row.get(expr._id).flatMap { d =>
//              Some((expr._id, d))
//            }
//          }.toMap
//
//          val dataInfoMap: Map[String, Any] = DataInfo.cacheInfoList.map { info =>
//            row.get(info.key) match {
//              case Some(d) => (info.key -> d)
//              case _ => info.defWrap
//            }
//          }.toMap
//
//          val groupbyData: Seq[AnyRef] = ruleExprs.groupbyExprs.flatMap { expr =>
//            expr.calculate(finalExprValueMap) match {
//              case Some(v) => Some(v.asInstanceOf[AnyRef])
//              case _ => None
//            }
//          }
//          val key = toTuple(groupbyData)
//
//          Some((key, (finalExprValueMap, dataInfoMap)))
//        }
//      }
//      case Failure(ex) => throw ex
//    }
//  }
//
//  override def cleanOldData(): Unit = {
//    cacheDataConnector.cleanOldData
//  }
//
//  override def updateOldData(t: Long, oldData: Iterable[Map[String, Any]]): Unit = {
//    if (dataConnectorParam.getMatchOnce) {
//      cacheDataConnector.updateOldData(t, oldData)
//    }
//  }
//
//  override def updateAllOldData(oldRdd: RDD[Map[String, Any]]): Unit = {
//    if (dataConnectorParam.getMatchOnce) {
//      cacheDataConnector.updateAllOldData(oldRdd)
//    }
//  }
//
//  private def toTuple[A <: AnyRef](as: Seq[A]): Product = {
//    if (as.size > 0) {
//      val tupleClass = Class.forName("scala.Tuple" + as.size)
//      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
//    } else None
//  }
//
//}
