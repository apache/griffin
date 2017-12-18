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
package org.apache.griffin.measure.rule.adaptor

import org.apache.griffin.measure.config.params.Param
import org.apache.griffin.measure.config.params.user.UserParam
import org.apache.griffin.measure.config.reader.ParamReaderFactory
import org.apache.griffin.measure.process._
import org.apache.griffin.measure.process.temp._
import org.apache.griffin.measure.rule.step.TimeInfo
import org.apache.griffin.measure.utils.JsonUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalamock.scalatest.MockFactory

import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class RuleAdaptorGroupTest extends FunSuite with Matchers with BeforeAndAfter with MockFactory {

  test ("profiling groupby") {
    RuleAdaptorGroup.init(
      "source" :: "target" :: Nil,
      "source",
      "coalesce" :: "count" :: "upper" :: Nil
    )
    TempTables.registerTempTableNameOnly(TempKeys.key(123), "source")
    TempTables.registerTempTableNameOnly(TempKeys.key(123), "target")

    val confFile = "src/test/resources/config-test-accuracy-new.json"

    val userParam = readParamFile[UserParam](confFile, "local") match {
      case Success(p) => p
      case Failure(ex) => fail
    }

    val dsTmsts = Map[String, Set[Long]](("source" -> Set[Long](111, 222, 333)))

    val steps = RuleAdaptorGroup.genRuleSteps(
      TimeInfo(123, 321),
      userParam.evaluateRuleParam,
      dsTmsts
    )
    steps.foreach(println)
  }

  private def readParamFile[T <: Param](file: String, fsType: String)(implicit m : Manifest[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file, fsType)
    paramReader.readConfig[T]
  }

}
