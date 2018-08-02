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
package org.apache.griffin.measure.configuration.dqdefinition

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.commons.lang.StringUtils
import org.apache.griffin.measure.configuration.enums._

/**
  * dq param
  * @param name         name of dq measurement (must)
  * @param timestamp    default timestamp of measure in batch mode (optional)
  * @param procType     batch mode or streaming mode (must)
  * @param dataSources   data sources (must)
  * @param evaluateRule  dq measurement (must)
  */
@JsonInclude(Include.NON_NULL)
case class DQConfig(@JsonProperty("name") name: String,
                    @JsonProperty("timestamp") timestamp: Long,
                    @JsonProperty("process.type") procType: String,
                    @JsonProperty("data.sources") dataSources: List[DataSourceParam],
                    @JsonProperty("evaluate.rule") evaluateRule: EvaluateRuleParam
                  ) extends Param {
  def getName: String = name
  def getTimestampOpt: Option[Long] = if (timestamp != 0) Some(timestamp) else None
  def getProcType: String = procType
  def getDataSources: Seq[DataSourceParam] = {
    dataSources.foldLeft((Nil: Seq[DataSourceParam], Set[String]())) { (ret, ds) =>
      val (seq, names) = ret
      if (!names.contains(ds.getName)){
        (seq :+ ds, names + ds.getName)
      } else ret
    }._1
  }
  def getEvaluateRule: EvaluateRuleParam = evaluateRule

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(name), "dq config name should not be blank")
    assert(StringUtils.isNotBlank(procType), "process.type should not be blank")
    assert((dataSources != null), "data.sources should not be null")
    assert((evaluateRule != null), "evaluate.rule should not be null")
    getDataSources.foreach(_.validate)
    evaluateRule.validate
  }
}

/**
  * data source param
  * @param name         data source name (must)
  * @param baseline     data source is baseline or not, false by default (optional)
  * @param connectors   data connectors (optional)
  * @param checkpoint   data source checkpoint configuration (must in streaming mode with streaming connectors)
  */
@JsonInclude(Include.NON_NULL)
case class DataSourceParam( @JsonProperty("name") name: String,
                            @JsonProperty("baseline") baseline: Boolean,
                            @JsonProperty("connectors") connectors: List[DataConnectorParam],
                            @JsonProperty("checkpoint") checkpoint: Map[String, Any]
                          ) extends Param {
  def getName: String = name
  def isBaseline: Boolean = if (baseline != null) baseline else false
  def getConnectors: Seq[DataConnectorParam] = if (connectors != null) connectors else Nil
  def getCheckpointOpt: Option[Map[String, Any]] = if (checkpoint != null) Some(checkpoint) else None

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(name), "data source name should not be empty")
    getConnectors.foreach(_.validate)
  }
}

/**
  * data connector param
  * @param conType    data connector type, e.g.: hive, avro, kafka (must)
  * @param version    data connector type version (optional)
  * @param dataFrameName    data connector dataframe name, for pre-process input usage (optional)
  * @param config     detail configuration of data connector (must)
  * @param preProc    pre-process rules after load data (optional)
  */
@JsonInclude(Include.NON_NULL)
case class DataConnectorParam( @JsonProperty("type") conType: String,
                               @JsonProperty("version") version: String,
                               @JsonProperty("dataframe.name") dataFrameName: String,
                               @JsonProperty("config") config: Map[String, Any],
                               @JsonProperty("pre.proc") preProc: List[RuleParam]
                             ) extends Param {
  def getType: String = conType
  def getVersion: String = if (version != null) version else ""
  def getDataFrameName(defName: String): String = if (dataFrameName != null) dataFrameName else defName
  def getConfig: Map[String, Any] = if (config != null) config else Map[String, Any]()
  def getPreProcRules: Seq[RuleParam] = if (preProc != null) preProc else Nil

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(conType), "data connector type should not be empty")
    getPreProcRules.foreach(_.validate)
  }
}

/**
  * evaluate rule param
  * @param rules      rules to define dq measurement (optional)
  */
@JsonInclude(Include.NON_NULL)
case class EvaluateRuleParam( @JsonProperty("rules") rules: List[RuleParam]
                            ) extends Param {
  def getRules: Seq[RuleParam] = if (rules != null) rules else Nil

  def validate(): Unit = {
    getRules.foreach(_.validate)
  }
}

/**
  * rule param
  * @param dslType    dsl type of this rule (must)
  * @param dqType     dq type of this rule (must if dsl type is "griffin-dsl")
  * @param inDfName       name of input dataframe of this rule, by default will be the previous rule output dataframe name
  * @param outDfName      name of output dataframe of this rule, by default will be generated as data connector dataframe name with index suffix
  * @param rule       rule to define dq step calculation (must)
  * @param details    detail config of rule (optional)
  * @param cache      cache the result for multiple usage (optional, valid for "spark-sql" and "df-ops" mode)
  * @param outputs    output ways configuration (optional)
//  * @param metric     config for metric output (optional)
//  * @param record     config for record output (optional)
//  * @param dsCacheUpdate    config for data source cache update output (optional, valid in streaming mode)
  */
@JsonInclude(Include.NON_NULL)
case class RuleParam(@JsonProperty("dsl.type") dslType: String,
                     @JsonProperty("dq.type") dqType: String,
                     @JsonProperty("in.dataframe.name") inDfName: String,
                     @JsonProperty("out.dataframe.name") outDfName: String,
                     @JsonProperty("rule") rule: String,
                     @JsonProperty("details") details: Map[String, Any],
                     @JsonProperty("cache") cache: Boolean,
                     @JsonProperty("out") outputs: List[RuleOutputParam]
                    ) extends Param {
  def getDslType: DslType = if (dslType != null) DslType(dslType) else DslType("")
  def getDqType: DqType = if (dqType != null) DqType(dqType) else DqType("")
  def getCache: Boolean = if (cache) cache else false

  def getInDfName(defName: String = ""): String = if (inDfName != null) inDfName else defName
  def getOutDfName(defName: String = ""): String = if (outDfName != null) outDfName else defName
  def getRule: String = if (rule != null) rule else ""
  def getDetails: Map[String, Any] = if (details != null) details else Map[String, Any]()

  def getOutputs: Seq[RuleOutputParam] = if (outputs != null) outputs else Nil
  def getOutputOpt(tp: OutputType): Option[RuleOutputParam] = getOutputs.filter(_.getOutputType == tp).headOption

  def replaceInDfName(newName: String): RuleParam = {
    if (StringUtils.equals(newName, inDfName)) this
    else RuleParam(dslType, dqType, newName, outDfName, rule, details, cache, outputs)
  }
  def replaceOutDfName(newName: String): RuleParam = {
    if (StringUtils.equals(newName, outDfName)) this
    else RuleParam(dslType, dqType, inDfName, newName, rule, details, cache, outputs)
  }
  def replaceInOutDfName(in: String, out: String): RuleParam = {
    if (StringUtils.equals(inDfName, in) && StringUtils.equals(outDfName, out)) this
    else RuleParam(dslType, dqType, in, out, rule, details, cache, outputs)
  }
  def replaceRule(newRule: String): RuleParam = {
    if (StringUtils.equals(newRule, rule)) this
    else RuleParam(dslType, dqType, inDfName, outDfName, newRule, details, cache, outputs)
  }

  def validate(): Unit = {
    assert(!(getDslType.equals(GriffinDslType) && getDqType.equals(UnknownType)),
      "unknown dq type for griffin dsl")

    getOutputs.foreach(_.validate)
  }
}

/**
  * out param of rule
  * @param outputType     output type (must)
  * @param name           output name (optional)
  * @param flatten        flatten type of output metric (optional, available in output metric type)
  */
@JsonInclude(Include.NON_NULL)
case class RuleOutputParam( @JsonProperty("type") outputType: String,
                            @JsonProperty("name") name: String,
                            @JsonProperty("flatten") flatten: String
                          ) extends Param {
  def getOutputType: OutputType = if (outputType != null) OutputType(outputType) else OutputType("")
  def getNameOpt: Option[String] = if (StringUtils.isNotBlank(name)) Some(name) else None
  def getFlatten: FlattenType = if (StringUtils.isNotBlank(flatten)) FlattenType(flatten) else FlattenType("")

  def validate(): Unit = {}
}