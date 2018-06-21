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
import org.apache.griffin.measure.utils.TimeUtil

/**
  * environment param
  * @param sparkParam       config of spark environment (must)
  * @param persistParams    config of persist ways (optional)
  * @param offsetCacheParams  config of information cache ways (required in streaming mode)
  */
@JsonInclude(Include.NON_NULL)
case class EnvConfig(@JsonProperty("spark") sparkParam: SparkParam,
                     @JsonProperty("persist") persistParams: List[PersistParam],
                     @JsonProperty("info.cache") offsetCacheParams: List[OffsetCacheParam]
                   ) extends Param {
  def getSparkParam: SparkParam = sparkParam
  def getPersistParams: Seq[PersistParam] = if (persistParams != null) persistParams else Nil
  def getOffsetCacheParams: Seq[OffsetCacheParam] = if (offsetCacheParams != null) offsetCacheParams else Nil

  def validate(): Unit = {
    assert((sparkParam != null), "spark param should not be null")
    sparkParam.validate
    getPersistParams.foreach(_.validate)
    getOffsetCacheParams.foreach(_.validate)
  }
}

/**
  * spark param
  * @param logLevel         log level of spark application (optional)
  * @param cpDir            checkpoint directory for spark streaming (required in streaming mode)
  * @param batchInterval    batch interval for spark streaming (required in streaming mode)
  * @param processInterval  process interval for streaming dq calculation (required in streaming mode)
  * @param config           extra config for spark environment (optional)
  * @param initClear        clear checkpoint directory or not when initial (optional)
  */
@JsonInclude(Include.NON_NULL)
case class SparkParam( @JsonProperty("log.level") logLevel: String,
                       @JsonProperty("checkpoint.dir") cpDir: String,
                       @JsonProperty("batch.interval") batchInterval: String,
                       @JsonProperty("process.interval") processInterval: String,
                       @JsonProperty("config") config: Map[String, String],
                       @JsonProperty("init.clear") initClear: Boolean
                     ) extends Param {
  def getLogLevel: String = if (logLevel != null) logLevel else "WARN"
  def getCpDir: String = if (cpDir != null) cpDir else ""
  def getBatchInterval: String = if (batchInterval != null) batchInterval else ""
  def getProcessInterval: String = if (processInterval != null) processInterval else ""
  def getConfig: Map[String, String] = if (config != null) config else Map[String, String]()
  def needInitClear: Boolean = if (initClear) initClear else false

  def validate(): Unit = {
//    assert(StringUtils.isNotBlank(cpDir), "checkpoint.dir should not be empty")
//    assert(TimeUtil.milliseconds(getBatchInterval).nonEmpty, "batch.interval should be valid time string")
//    assert(TimeUtil.milliseconds(getProcessInterval).nonEmpty, "process.interval should be valid time string")
  }
}

/**
  * persist param
  * @param persistType    persist type, e.g.: log, hdfs, http, mongo (must)
  * @param config         config of persist way (must)
  */
@JsonInclude(Include.NON_NULL)
case class PersistParam( @JsonProperty("type") persistType: String,
                         @JsonProperty("config") config: Map[String, Any]
                       ) extends Param {
  def getType: String = persistType
  def getConfig: Map[String, Any] = if (config != null) config else Map[String, Any]()

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(persistType), "persist type should not be empty")
  }
}

/**
  * offset cache param
  * @param cacheType    offset cache type, e.g.: zookeeper (must)
  * @param config       config of cache way
  */
@JsonInclude(Include.NON_NULL)
case class OffsetCacheParam(@JsonProperty("type") cacheType: String,
                            @JsonProperty("config") config: Map[String, Any]
                          ) extends Param {
  def getType: String = cacheType
  def getConfig: Map[String, Any] = if (config != null) config else Map[String, Any]()

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(cacheType), "info cache type should not be empty")
  }
}