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
//package org.apache.griffin.measure.cache
//
//import java.util.Date
//import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}
//
//import org.apache.curator.framework.recipes.locks.InterProcessMutex
//import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
//import org.apache.curator.retry.ExponentialBackoffRetry
//import org.apache.griffin.measure.cache.info.InfoCacheInstance
//import org.apache.griffin.measure.config.params.env.InfoCacheParam
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//import scala.util.{Failure, Try}
//
//@RunWith(classOf[JUnitRunner])
//class InfoCacheInstanceTest extends FunSuite with Matchers with BeforeAndAfter {
//
//  val map = Map[String, Any](
//    ("hosts" -> "localhost:2181"),
//    ("namespace" -> "griffin/infocache"),
//    ("lock.path" -> "lock"),
//    ("mode" -> "persist"),
//    ("init.clear" -> true),
//    ("close.clear" -> false)
//  )
//  val name = "ttt"
//
//  val icp = InfoCacheParam("zk", map)
//  val icps = icp :: Nil
//
//  before {
//    InfoCacheInstance.initInstance(icps, name)
//    InfoCacheInstance.init
//  }
//
//  test ("others") {
//    InfoCacheInstance.available should be (true)
//
//    val keys = List[String](
//      "key1", "key2"
//    )
//    val info = Map[String, String](
//      ("key1" -> "value1"),
//      ("key2" -> "value2")
//    )
//
//    InfoCacheInstance.cacheInfo(info) should be (true)
//    InfoCacheInstance.readInfo(keys) should be (info)
//    InfoCacheInstance.deleteInfo(keys)
////    InfoCacheInstance.readInfo(keys) should be (Map[String, String]())
//
//  }
//
//  after {
//    InfoCacheInstance.close()
//  }
//
//}
