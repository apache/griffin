package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class TypeConfigParam( @JsonProperty("dq") dqType: String,
                            @JsonProperty("dataAsset") dataAssetTypeMap: Map[String, String]
                          ) extends ConfigParam {

}