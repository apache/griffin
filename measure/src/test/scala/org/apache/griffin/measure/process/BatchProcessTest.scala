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
//package org.apache.griffin.measure.process
//
//import org.apache.griffin.measure.config.params.env._
//import org.apache.griffin.measure.config.params.user._
//import org.apache.griffin.measure.config.params._
//import org.apache.griffin.measure.config.reader.ParamReaderFactory
//import org.apache.griffin.measure.config.validator.AllParamValidator
//import org.apache.griffin.measure.log.Loggable
//import org.apache.griffin.measure.persist.PersistThreadPool
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//import scala.util.{Failure, Success, Try}
//
//@RunWith(classOf[JUnitRunner])
//class BatchProcessTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {
//
//  val envFile = "src/test/resources/env-test.json"
//  val confFile = "src/test/resources/config-test-profiling.json"
////  val confFile = "src/test/resources/config-test-accuracy.json"
//
//  val envFsType = "local"
//  val userFsType = "local"
//
//  val args = Array(envFile, confFile)
//
//  var allParam: AllParam = _
//
//  before {
//    // read param files
//    val envParam = readParamFile[EnvParam](envFile, envFsType) match {
//      case Success(p) => p
//      case Failure(ex) => {
//        error(ex.getMessage)
//        sys.exit(-2)
//      }
//    }
//    val userParam = readParamFile[UserParam](confFile, userFsType) match {
//      case Success(p) => p
//      case Failure(ex) => {
//        error(ex.getMessage)
//        sys.exit(-2)
//      }
//    }
//    allParam = AllParam(envParam, userParam)
//
//    // validate param files
//    validateParams(allParam) match {
//      case Failure(ex) => {
//        error(ex.getMessage)
//        sys.exit(-3)
//      }
//      case _ => {
//        info("params validation pass")
//      }
//    }
//  }
//
//  test ("batch process") {
//    val procType = ProcessType(allParam.userParam.procType)
//    val proc: DqProcess = procType match {
//      case BatchProcessType => BatchDqProcess(allParam)
//      case StreamingProcessType => StreamingDqProcess(allParam)
//      case _ => {
//        error(s"${procType} is unsupported process type!")
//        sys.exit(-4)
//      }
//    }
//
//    // process init
//    proc.init match {
//      case Success(_) => {
//        info("process init success")
//      }
//      case Failure(ex) => {
//        error(s"process init error: ${ex.getMessage}")
//        shutdown
//        sys.exit(-5)
//      }
//    }
//
//    // process run
//    proc.run match {
//      case Success(_) => {
//        info("process run success")
//      }
//      case Failure(ex) => {
//        error(s"process run error: ${ex.getMessage}")
//
//        if (proc.retriable) {
//          throw ex
//        } else {
//          shutdown
//          sys.exit(-5)
//        }
//      }
//    }
//
//    // process end
//    proc.end match {
//      case Success(_) => {
//        info("process end success")
//      }
//      case Failure(ex) => {
//        error(s"process end error: ${ex.getMessage}")
//        shutdown
//        sys.exit(-5)
//      }
//    }
//
//    shutdown
//  }
//
//  private def readParamFile[T <: Param](file: String, fsType: String)(implicit m : Manifest[T]): Try[T] = {
//    val paramReader = ParamReaderFactory.getParamReader(file, fsType)
//    paramReader.readConfig[T]
//  }
//
//  private def validateParams(allParam: AllParam): Try[Boolean] = {
//    val allParamValidator = AllParamValidator()
//    allParamValidator.validate(allParam)
//  }
//
//  private def shutdown(): Unit = {
//    PersistThreadPool.shutdown
//  }
//}
