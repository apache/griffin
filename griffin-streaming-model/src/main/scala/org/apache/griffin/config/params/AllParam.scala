package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class AllParam( @JsonProperty("spark") sparkParam: SparkParam,
                     @JsonProperty("type.config") typeConfigParam: TypeConfigParam,
                     @JsonProperty("dataAsset") dataAssetParamMap: Map[String, DataAssetParam],
                     @JsonProperty("dq.config") dqConfigParam: DqConfigParam,
                     @JsonProperty("recorder") recorderParam: RecorderParam,
                     @JsonProperty("retry") retryParam: RetryParam
                   ) extends ConfigParam {

}
