package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class AccuracyParam( @JsonProperty("mapping") mapping: List[MappingParam],
                          @JsonProperty("match.valid.time") matchValidTime: TimeRangeParam
                        ) extends ConfigParam {

}
