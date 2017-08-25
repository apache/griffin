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
package org.apache.griffin.measure.process.engine

import org.apache.griffin.measure.config.params.user.DataSourceParam
import org.apache.griffin.measure.data.source._
import org.apache.griffin.measure.log.Loggable
import org.apache.griffin.measure.persist.Persist
import org.apache.griffin.measure.rules.step._

case class DqEngines(engines: Seq[DqEngine]) extends DqEngine {

  def initDataSources(dataSourceParams: Seq[DataSourceParam]): Unit = {
    val dataSources = dataSourceParams.flatMap { param =>
      genDataSource(param)
    }
    dataSources.foreach { ds =>
      ds.init
    }
  }

  def runRuleSteps(ruleSteps: Seq[ConcreteRuleStep]): Unit = {
    ruleSteps.foreach { ruleStep =>
      runRuleStep(ruleStep)
    }
  }

  def persistResults(ruleSteps: Seq[ConcreteRuleStep], persist: Persist): Unit = {
    ruleSteps.foreach { ruleStep =>
      persistResult(ruleStep, persist)
    }
  }

  def genDataSource(dataSourceParam: DataSourceParam): Option[DataSource] = {
    val ret = engines.foldLeft(None: Option[DataSource]) { (dsOpt, engine) =>
      if (dsOpt.isEmpty) engine.genDataSource(dataSourceParam) else dsOpt
    }
    if (ret.isEmpty) warn(s"init data source warn: no dq engine support ${dataSourceParam}")
    ret
  }

  def runRuleStep(ruleStep: ConcreteRuleStep): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.runRuleStep(ruleStep)
    }
    if (!ret) warn(s"run rule step warn: no dq engine support ${ruleStep}")
    ret
  }

  def persistResult(ruleStep: ConcreteRuleStep, persist: Persist): Boolean = {
    val ret = engines.foldLeft(false) { (done, engine) =>
      done || engine.persistResult(ruleStep, persist)
    }
    if (!ret) error(s"persist result warn: no dq engine support ${ruleStep}")
    ret
  }

}
