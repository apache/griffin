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
//import org.apache.griffin.measure.data.connector.cache.CacheDataConnector
//import org.apache.griffin.measure.data.connector.streaming.StreamingDataConnector
//import org.apache.griffin.measure.result.{DataInfo, TimeStampInfo}
//import org.apache.griffin.measure.rule.ExprValueUtil
//import org.apache.spark.rdd.RDD
//
//import scala.util.{Failure, Success}
//
//trait StreamingCacheDirectDataConnector extends DirectDataConnector {
//
//  val cacheDataConnector: CacheDataConnector
//  @transient val streamingDataConnector: StreamingDataConnector
//
//  def available(): Boolean = {
//    cacheDataConnector.available && streamingDataConnector.available
//  }
//
//  def init(): Unit = {
//    cacheDataConnector.init
//
//    val ds = streamingDataConnector.stream match {
//      case Success(dstream) => dstream
//      case Failure(ex) => throw ex
//    }
//
//    ds.foreachRDD((rdd, time) => {
//      val ms = time.milliseconds
//
//      val valueMapRdd = transform(rdd, ms)
//
//      // save data frame
//      cacheDataConnector.saveData(valueMapRdd, ms)
//    })
//  }
//
//  protected def transform(rdd: RDD[(streamingDataConnector.K, streamingDataConnector.V)],
//                          ms: Long
//                         ): RDD[Map[String, Any]]
//
//}
