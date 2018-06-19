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
  * @param connectors   data connectors (optional)
  * @param cache        data source cache configuration (must in streaming mode with streaming connectors)
  */
@JsonInclude(Include.NON_NULL)
case class DataSourceParam( @JsonProperty("name") name: String,
                            @JsonProperty("connectors") connectors: List[DataConnectorParam],
                            @JsonProperty("cache") cache: Map[String, Any]
                          ) extends Param {
  def getName: String = name
  def getConnectors: Seq[DataConnectorParam] = if (connectors != null) connectors else Nil
  def getCacheOpt: Option[Map[String, Any]] = if (cache != null) Some(cache) else None

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(name), "data source name should not be empty")
    getConnectors.foreach(_.validate)
  }
}

/**
  * data connector param
  * @param conType    data connector type, e.g.: hive, avro, kafka (must)
  * @param version    data connector type version (optional)
  * @param config     detail configuration of data connector (must)
  * @param preProc    pre-process rules after load data (optional)
  */
@JsonInclude(Include.NON_NULL)
case class DataConnectorParam( @JsonProperty("type") conType: String,
                               @JsonProperty("version") version: String,
                               @JsonProperty("config") config: Map[String, Any],
                               @JsonProperty("pre.proc") preProc: List[RuleParam]
                             ) extends Param {
  def getType: String = conType
  def getVersion: String = version
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
  * @param name       name of result calculated by this rule (must if for later usage)
  * @param rule       rule to define dq step calculation (must)
  * @param details    detail config of rule (optional)
  * @param cache      cache the result for multiple usage (optional, valid for "spark-sql" and "df-opr" mode)
  * @param metric     config for metric output (optional)
  * @param record     config for record output (optional)
  * @param dsCacheUpdate    config for data source cache update output (optional, valid in streaming mode)
  */
@JsonInclude(Include.NON_NULL)
case class RuleParam( @JsonProperty("dsl.type") dslType: String,
                      @JsonProperty("dq.type") dqType: String,
                      @JsonProperty("name") name: String,
                      @JsonProperty("rule") rule: String,
                      @JsonProperty("details") details: Map[String, Any],
                      @JsonProperty("cache") cache: Boolean,
                      @JsonProperty("metric") metric: RuleMetricParam,
                      @JsonProperty("record") record: RuleRecordParam,
                      @JsonProperty("ds.cache.update") dsCacheUpdate: RuleDsCacheUpdateParam
                    ) extends Param {
  def getDslType: DslType = if (dslType != null) DslType(dslType) else DslType("")
  def getDqType: DqType = if (dqType != null) DqType(dqType) else DqType("")
  def getCache: Boolean = if (cache) cache else false

  def getName: String = if (name != null) name else ""
  def getRule: String = if (rule != null) rule else ""
  def getDetails: Map[String, Any] = if (details != null) details else Map[String, Any]()

  def getMetricOpt: Option[RuleMetricParam] = if (metric != null) Some(metric) else None
  def getRecordOpt: Option[RuleRecordParam] = if (record != null) Some(record) else None
  def getDsCacheUpdateOpt: Option[RuleDsCacheUpdateParam] = if (dsCacheUpdate != null) Some(dsCacheUpdate) else None

  def replaceName(newName: String): RuleParam = {
    if (StringUtils.equals(newName, name)) this
    else RuleParam(dslType, dqType, newName, rule, details, cache, metric, record, dsCacheUpdate)
  }
  def replaceRule(newRule: String): RuleParam = {
    if (StringUtils.equals(newRule, rule)) this
    else RuleParam(dslType, dqType, name, newRule, details, cache, metric, record, dsCacheUpdate)
  }
  def replaceDetails(newDetails: Map[String, Any]): RuleParam = {
    RuleParam(dslType, dqType, name, rule, newDetails, cache, metric, record, dsCacheUpdate)
  }

  def validate(): Unit = {
    assert(!(getDslType.equals(GriffinDslType) && getDqType.equals(UnknownType)),
      "unknown dq type for griffin dsl")

    getMetricOpt.foreach(_.validate)
    getRecordOpt.foreach(_.validate)
    getDsCacheUpdateOpt.foreach(_.validate)
  }
}

/**
  * metric param of rule
  * @param name         name of metric to output (optional)
  * @param collectType  the normalize strategy to collect metric  (optional)
  */
@JsonInclude(Include.NON_NULL)
case class RuleMetricParam( @JsonProperty("name") name: String,
                            @JsonProperty("collect.type") collectType: String
                          ) extends Param {
  def getNameOpt: Option[String] = if (StringUtils.isNotBlank(name)) Some(name) else None
  def getCollectType: NormalizeType = if (StringUtils.isNotBlank(collectType)) NormalizeType(collectType) else NormalizeType("")

  def validate(): Unit = {}
}

/**
  * record param of rule
  * @param name   name of record to output (optional)
  */
@JsonInclude(Include.NON_NULL)
case class RuleRecordParam( @JsonProperty("name") name: String
                          ) extends Param {
  def getNameOpt: Option[String] = if (StringUtils.isNotBlank(name)) Some(name) else None

  def validate(): Unit = {}
}

/**
  * data source cache update param of rule
  * @param dsName   name of data source to be updated by thie rule result (must)
  */
@JsonInclude(Include.NON_NULL)
case class RuleDsCacheUpdateParam( @JsonProperty("ds.name") dsName: String
                                 ) extends Param {
  def getDsNameOpt: Option[String] = if (StringUtils.isNotBlank(dsName)) Some(dsName) else None

  def validate(): Unit = {}
}