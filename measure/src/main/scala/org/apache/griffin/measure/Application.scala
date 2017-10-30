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

import org.apache.griffin.measure.config.params._
import org.apache.griffin.measure.config.params.env._
import org.apache.griffin.measure.config.params.user._
import org.apache.griffin.measure.config.reader._
import org.apache.griffin.measure.config.validator.AllParamValidator
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.PersistThreadPool
import org.apache.griffin.measure.process._

import scala.util.{Failure, Success, Try}

object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    info(args.toString)
    if (args.length < 2) {
      error("Usage: class <env-param> <user-param> [List of String split by comma: raw | local | hdfs(default)]")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val userParamFile = args(1)
    val (envFsType, userFsType) = if (args.length > 2) {
      val fsTypes = args(2).trim.split(",")
      if (fsTypes.length == 1) (fsTypes(0).trim, fsTypes(0).trim)
      else if (fsTypes.length >= 2) (fsTypes(0).trim, fsTypes(1).trim)
      else ("hdfs", "hdfs")
    } else ("hdfs", "hdfs")

    info(envParamFile)
    info(userParamFile)

    // read param files
    val envParam = readParamFile[EnvParam](envParamFile, envFsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val userParam = readParamFile[UserParam](userParamFile, userFsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val allParam: AllParam = AllParam(envParam, userParam)

    // validate param files
    validateParams(allParam) match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-3)
      }
      case _ => {
        info("params validation pass")
      }
    }

    // choose algorithm
//    val dqType = allParam.userParam.dqType
    val procType = ProcessType(allParam.userParam.procType)
    val proc: DqProcess = procType match {
      case BatchProcessType => BatchDqProcess(allParam)
      case StreamingProcessType => StreamingDqProcess(allParam)
      case _ => {
        error(s"${procType} is unsupported process type!")
        sys.exit(-4)
      }
    }

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

        if (proc.retriable) {
          throw ex
        } else {
          shutdown
          sys.exit(-5)
        }
      }
    }

    // process end
    proc.end match {
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

//    val algo: Algo = (dqType, procType) match {
//      case (MeasureType.accuracy(), ProcessType.batch()) => BatchAccuracyAlgo(allParam)
//      case (MeasureType.profile(), ProcessType.batch()) => BatchProfileAlgo(allParam)
//      case (MeasureType.accuracy(), ProcessType.streaming()) => StreamingAccuracyAlgo(allParam)
////      case (MeasureType.profile(), ProcessType.streaming()) => StreamingProfileAlgo(allParam)
//      case _ => {
//        error(s"${dqType} with ${procType} is unsupported dq type!")
//        sys.exit(-4)
//      }
//    }

    // algorithm run
//    algo.run match {
//      case Failure(ex) => {
//        error(s"app error: ${ex.getMessage}")
//
//        procType match {
//          case ProcessType.streaming() => {
//            // streaming need to attempt more times by spark streaming itself
//            throw ex
//          }
//          case _ => {
//            shutdown
//            sys.exit(-5)
//          }
//        }
//      }
//      case _ => {
//        info("app finished and success")
//      }
//    }
  }

  private def readParamFile[T <: Param](file: String, fsType: String)(implicit m : Manifest[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file, fsType)
    paramReader.readConfig[T]
  }

  private def validateParams(allParam: AllParam): Try[Boolean] = {
    val allParamValidator = AllParamValidator()
    allParamValidator.validate(allParam)
  }

  private def shutdown(): Unit = {
    PersistThreadPool.shutdown
  }

}
