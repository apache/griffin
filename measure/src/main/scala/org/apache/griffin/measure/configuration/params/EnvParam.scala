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

/**
  * environment param
  * @param sparkParam       config of spark environment (must)
  * @param persistParams    config of persist ways (optional)
  * @param infoCacheParams  config of information cache ways (must in streaming mode)
  * @param cleanerParam     config of cleaner (optional)
  */
@JsonInclude(Include.NON_NULL)
case class EnvParam( @JsonProperty("spark") sparkParam: SparkParam,
                     @JsonProperty("persist") persistParams: List[PersistParam],
                     @JsonProperty("info.cache") infoCacheParams: List[InfoCacheParam],
                     @JsonProperty("cleaner") cleanerParam: CleanerParam
                   ) extends Param {
}

/**
  * spark param
  * @param logLevel         log level of spark application (optional)
  * @param cpDir            checkpoint directory for spark streaming (must in streaming mode)
  * @param batchInterval    batch interval for spark streaming (must in streaming mode)
  * @param processInterval  process interval for streaming dq calculation (must in streaming mode)
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
  def needInitClear: Boolean = if (initClear != null) initClear else false
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
  override def validate(): Boolean = {
    StringUtils.isNotBlank(persistType)
  }
}

/**
  * info cache param
  * @param cacheType    information cache type, e.g.: zookeeper (must)
  * @param config       config of cache way
  */
@JsonInclude(Include.NON_NULL)
case class InfoCacheParam( @JsonProperty("type") cacheType: String,
                           @JsonProperty("config") config: Map[String, Any]
                         ) extends Param {
  override def validate(): Boolean = {
    StringUtils.isNotBlank(cacheType)
  }
}

/**
  * cleaner param, invalid at current
  * @param cleanInterval    clean interval (optional)
  */
@JsonInclude(Include.NON_NULL)
case class CleanerParam( @JsonProperty("clean.interval") cleanInterval: String
                       ) extends Param {

}