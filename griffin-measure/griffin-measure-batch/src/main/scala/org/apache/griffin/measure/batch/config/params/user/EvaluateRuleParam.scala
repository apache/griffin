package org.apache.griffin.measure.batch.config.params.user

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.batch.config.params.Param

@JsonInclude(Include.NON_NULL)
case class EvaluateRuleParam( @JsonProperty("sampleRatio") sampleRatio: Double,
                              @JsonProperty("rules") rules: String
                            ) extends Param {

}
