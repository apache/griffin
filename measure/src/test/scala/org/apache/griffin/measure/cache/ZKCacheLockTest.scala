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
//import org.apache.griffin.measure.cache.info.ZKInfoCache
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//import scala.util.{Failure, Try}
//
//@RunWith(classOf[JUnitRunner])
//class ZKCacheLockTest extends FunSuite with Matchers with BeforeAndAfter {
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
//  val ic = ZKInfoCache(map, name)
//
//  before {
//    ic.init
//  }
//
//  test ("lock") {
//
//    case class Proc(n: Int) extends Runnable {
//      override def run(): Unit = {
//        val cl = ic.genLock("proc")
//        val b = cl.lock(2, TimeUnit.SECONDS)
//        try {
//          println(s"${n}: ${b}")
//          if (b) Thread.sleep(3000)
//        } finally {
//          cl.unlock()
//        }
//      }
//    }
//
//    val pool = Executors.newFixedThreadPool(5).asInstanceOf[ThreadPoolExecutor]
//    val t = 0 until 10
//    t.foreach(a => pool.submit(Proc(a)))
//
//    pool.shutdown()
//    val t1 = new Date()
//    println(s"${t1}: pool shut down")
//    pool.awaitTermination(20, TimeUnit.SECONDS)
//    val t2 = new Date()
//    println(s"${t2}: pool shut down done [${t2.getTime - t1.getTime}]")
//  }
//
//  after {
//    ic.close()
//  }
//
//}
