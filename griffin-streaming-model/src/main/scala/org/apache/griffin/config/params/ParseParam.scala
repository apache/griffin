package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class ParseParam( @JsonProperty("in.type") inType: String,
                       @JsonProperty("out.type") outType: String,
                       @JsonProperty("config") configParam: ParseConfigParam
                     ) extends ConfigParam {

}
