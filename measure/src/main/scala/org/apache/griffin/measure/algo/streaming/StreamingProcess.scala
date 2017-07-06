/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.algo.streaming

import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}
import java.util.{Timer, TimerTask}

case class StreamingProcess(interval: Long, runnable: Runnable) {

  val pool: ThreadPoolExecutor = Executors.newFixedThreadPool(5).asInstanceOf[ThreadPoolExecutor]

  val timer = new Timer("process", true)

  val timerTask = new TimerTask() {
    override def run(): Unit = {
      pool.submit(runnable)
    }
  }

  def startup(): Unit = {
    timer.schedule(timerTask, 0, interval)
  }

  def shutdown(): Unit = {
    timer.cancel()
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS)
  }

}
