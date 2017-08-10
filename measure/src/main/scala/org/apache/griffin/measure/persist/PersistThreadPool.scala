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

import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}

object PersistThreadPool {

  private val pool: ThreadPoolExecutor = Executors.newFixedThreadPool(10).asInstanceOf[ThreadPoolExecutor]
  val MAX_RETRY = 100

  def shutdown(): Unit = {
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS)
  }

  def addTask(func: () => Boolean, retry: Int): Unit = {
    val r = if (retry < 0) MAX_RETRY else retry
    println(s"add task, current task num: ${pool.getQueue.size}")
    pool.submit(Task(func, r))
  }

  case class Task(func: () => Boolean, retry: Int) extends Runnable {

    override def run(): Unit = {
      try {
        var i = retry
        var suc = false
        while (!suc && i > 0) {
          if (func()) {
            println("task success")
            suc = true
          } else i = i - 1
        }
        if (!suc) fail(s"retried for ${retry} times")
      } catch {
        case e: Throwable => fail(s"${e.getMessage}")
      }
    }

    def fail(msg: String): Unit = {
      println(s"task fails: ${msg}")
    }
  }

}
