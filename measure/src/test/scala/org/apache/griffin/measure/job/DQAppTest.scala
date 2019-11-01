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
package org.apache.griffin.measure.job

import scala.util.Failure
import scala.util.Success

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import org.apache.griffin.measure.Application._
import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.SparkSuiteBase
import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.launch.streaming.StreamingDQApp

class DQAppTest extends FlatSpec with SparkSuiteBase with BeforeAndAfterAll with Matchers with Loggable {

  var envParam: EnvConfig = _
  var sparkParam: SparkParam = _

  var dqApp: DQApp = _

  def getConfigFilePath(fileName: String): String = {
    getClass.getResource(fileName).getFile
  }

  def initApp(dqParamFile: String): DQApp = {
    val dqParam = readParamFile[DQConfig](getConfigFilePath(dqParamFile)) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }

    val allParam: GriffinConfig = GriffinConfig(envParam, dqParam)

    // choose process
    val procType = ProcessType(allParam.getDqConfig.getProcType)
    dqApp = procType match {
      case BatchProcessType => BatchDQApp(allParam)
      case StreamingProcessType => StreamingDQApp(allParam)
      case _ =>
        error(s"${procType} is unsupported process type!")
        sys.exit(-4)
    }

    dqApp.sparkSession = spark
    dqApp
  }
}
