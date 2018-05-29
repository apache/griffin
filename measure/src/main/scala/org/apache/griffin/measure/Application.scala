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
package org.apache.griffin.measure

import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.configuration.json.ParamReaderFactory
import org.apache.griffin.measure.configuration.params.{AllParam, DQParam, EnvParam, Param}
import org.apache.griffin.measure.configuration.validator.ParamValidator
import org.apache.griffin.measure.context.writer.PersistTaskRunner
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.launch.streaming.StreamingDQApp

import scala.util.{Failure, Success, Try}

/**
  * application entrance
  */
object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    info(args.toString)
    if (args.length < 2) {
      error("Usage: class <env-param> <dq-param>")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val dqParamFile = args(1)

    info(envParamFile)
    info(dqParamFile)

    // read param files
    val envParam = readParamFile[EnvParam](envParamFile) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val dqParam = readParamFile[DQParam](dqParamFile) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val allParam: AllParam = AllParam(envParam, dqParam)

    // validate param files
    ParamValidator.validate(allParam) match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-3)
      }
      case _ => {
        info("params validation pass")
      }
    }

    // choose process
    val procType = ProcessType(allParam.dqParam.procType)
    val proc: DQApp = procType match {
      case BatchProcessType => BatchDQApp(allParam)
      case StreamingProcessType => StreamingDQApp(allParam)
      case _ => {
        error(s"${procType} is unsupported process type!")
        sys.exit(-4)
      }
    }

    startup

    // process init
    proc.init match {
      case Success(_) => {
        info("process init success")
      }
      case Failure(ex) => {
        error(s"process init error: ${ex.getMessage}")
        shutdown
        sys.exit(-5)
      }
    }

    // process run
    proc.run match {
      case Success(_) => {
        info("process run success")
      }
      case Failure(ex) => {
        error(s"process run error: ${ex.getMessage}")

        if (proc.retryable) {
          throw ex
        } else {
          shutdown
          sys.exit(-5)
        }
      }
    }

    // process end
    proc.close match {
      case Success(_) => {
        info("process end success")
      }
      case Failure(ex) => {
        error(s"process end error: ${ex.getMessage}")
        shutdown
        sys.exit(-5)
      }
    }

    shutdown
  }

  private def readParamFile[T <: Param](file: String)(implicit m : Manifest[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file)
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {
  }

  private def shutdown(): Unit = {
  }

}
