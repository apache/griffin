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

package org.apache.griffin.measure.configuration.dqdefinition.reader
import org.apache.griffin.measure.configuration.dqdefinition.DQConfig
import org.scalatest._


import scala.util.{Failure, Success}

class ParamFileReaderSpec extends FlatSpec with Matchers{


  "params " should "be parsed from a valid file" in {
    val reader :ParamReader = ParamFileReader(getClass.getResource("/_accuracy-batch-griffindsl.json").getFile)
    val params = reader.readConfig[DQConfig]
    params match {
      case Success(v) =>
        v.getEvaluateRule.getRules(0).getDslType.desc should === ("griffin-dsl")
        v.getEvaluateRule.getRules(0).getOutDfName() should === ("accu")
      case Failure(_) =>
        fail("it should not happen")
    }

  }

  it should "fail for an invalid file" in {
    val reader :ParamReader = ParamFileReader(getClass.getResource("/invalidconfigs/missingrule_accuracy_batch_sparksql.json").getFile)
    val params = reader.readConfig[DQConfig]
    params match {
      case Success(_) =>
        fail("it is an invalid config file")
      case Failure(e) =>
        e.getMessage contains ("evaluate.rule should not be null")
    }

  }

}
