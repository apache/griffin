package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class TimeRangeParam( @JsonProperty("begin") begin: Int,
                           @JsonProperty("end") end: Int,
                           @JsonProperty("unit") unit: String
                         ) extends ConfigParam {

}