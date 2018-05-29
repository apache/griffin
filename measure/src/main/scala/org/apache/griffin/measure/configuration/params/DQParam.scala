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
package org.apache.griffin.measure.configuration.params

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.commons.lang.StringUtils
import org.apache.griffin.measure.configuration.enums.{DqType, DslType}

/**
  * dq param
  * @param name         name of dq measurement (must)
  * @param timestamp    default timestamp of measure in batch mode (optional)
  * @param procType     batch mode or streaming mode (must)
  * @param dataSourceParams   data sources (optional)
  * @param evaluateRuleParam  dq measurement (optional)
  */
@JsonInclude(Include.NON_NULL)
case class DQParam( @JsonProperty("name") name: String,
                    @JsonProperty("timestamp") timestamp: Long,
                    @JsonProperty("process.type") procType: String,
                    @JsonProperty("data.sources") dataSourceParams: List[DataSourceParam],
                    @JsonProperty("evaluate.rule") evaluateRuleParam: EvaluateRuleParam
                  ) extends Param {
  val dataSources = {
    val (validDsParams, _) = dataSourceParams.foldLeft((Nil: Seq[DataSourceParam], Set[String]())) { (ret, dsParam) =>
      val (seq, names) = ret
      if (dsParam.hasName && !names.contains(dsParam.name)) {
        (seq :+ dsParam, names + dsParam.name)
      } else ret
    }
    validDsParams
  }
  val evaluateRule: EvaluateRuleParam = {
    if (evaluateRuleParam != null) evaluateRuleParam
    else EvaluateRuleParam("", Nil)
  }

  override def validate(): Boolean = {
    dataSources.nonEmpty
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
  def hasName: Boolean = StringUtils.isNotBlank(name)
  def getConnectors: List[DataConnectorParam] = if (connectors != null) connectors else Nil
  def hasCache: Boolean = (cache != null)

  override def validate(): Boolean = hasName
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
  override def validate(): Boolean = {
    StringUtils.isNotBlank(conType)
  }
}

/**
  * evaluate rule param
  * @param dslType    default dsl type for all rules (optional)
  * @param rules      rules to define dq measurement (optional)
  */
@JsonInclude(Include.NON_NULL)
case class EvaluateRuleParam( @JsonProperty("dsl.type") dslType: String,
                              @JsonProperty("rules") rules: List[RuleParam]
                            ) extends Param {
  def getDslType: DslType = if (dslType != null) DslType(dslType) else DslType("")
  def getRules: List[RuleParam] = if (rules != null) rules else Nil
}

/**
  * rule param
  * @param dslType    dsl type of this rule (must if default dsl type not set)
  * @param dqType     dq type of this rule (valid for "griffin-dsl")
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
  def getDslType(defaultDslType: DslType): DslType = if (dslType != null) DslType(dslType) else defaultDslType
  def getDqType: DqType = if (dqType != null) DqType(dqType) else DqType("")
  def getName: String = if (name != null) name else ""
  def getRule: String = if (rule != null) rule else ""
  def getDetails: Map[String, Any] = if (details != null) details else Map[String, Any]()
  def getCache: Boolean = if (cache != null) cache else false

  def metricOpt: Option[RuleMetricParam] = if (metric != null) Some(metric) else None
  def recordOpt: Option[RuleRecordParam] = if (record != null) Some(record) else None
  def dsCacheUpdateOpt: Option[RuleDsCacheUpdateParam] = if (dsCacheUpdate != null) Some(dsCacheUpdate) else None

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
}

/**
  * record param of rule
  * @param name   name of record to output (optional)
  */
@JsonInclude(Include.NON_NULL)
case class RuleRecordParam( @JsonProperty("name") name: String
                          ) extends Param {
}

/**
  * data source cache update param of rule
  * @param dsName   name of data source to be updated by thie rule result (must)
  */
@JsonInclude(Include.NON_NULL)
case class RuleDsCacheUpdateParam( @JsonProperty("ds.name") dsName: String
                                 ) extends Param {
}