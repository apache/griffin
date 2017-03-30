package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class SampleParam( @JsonProperty("need.sample") needSample: Boolean,
                        @JsonProperty("ratio") ratio: Double
                      ) extends ConfigParam {

}
