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
package org.apache.griffin.measure.persist

import java.util.Date
import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object MongoThreadPool {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val pool: ThreadPoolExecutor = Executors.newFixedThreadPool(5).asInstanceOf[ThreadPoolExecutor]
  val MAX_RETRY = 100

  def shutdown(): Unit = {
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS)
  }

  def addTask(func: () => (Long, Future[_]), retry: Int): Unit = {
    val r = if (retry < 0) MAX_RETRY else retry
    println(s"add task, current task num: ${pool.getQueue.size}")
    pool.submit(Task(func, r))
  }

  case class Task(func: () => (Long, Future[_]), retry: Int) extends Runnable {

    override def run(): Unit = {
      val st = new Date().getTime
      val (t, res) = func()
      res.onComplete {
        case Success(value) => {
          val et = new Date().getTime
          println(s"task ${t} success [ using time ${et - st} ms ]")
        }
        case Failure(e) => {
          val et = new Date().getTime
          println(s"task ${t} fails [ using time ${et - st} ms ] : ${e.getMessage}")
          if (retry > 0) {
            println(s"task ${t} retry [ rest retry count: ${retry - 1} ]")
            pool.submit(Task(func, retry - 1))
          } else {
            println(s"task ${t} retry ends but fails")
          }
        }
      }
    }

    def fail(): Unit = {
      println("task fails")
    }
  }

}
