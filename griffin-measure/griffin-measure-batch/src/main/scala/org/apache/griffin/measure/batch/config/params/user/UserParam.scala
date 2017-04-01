package org.apache.griffin.measure.batch.config.params.user

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.batch.config.params.Param

@JsonInclude(Include.NON_NULL)
case class UserParam( @JsonProperty("name") name: String,
                      @JsonProperty("type") dqType: String,
                      @JsonProperty("source") sourceParam: DataParam,
                      @JsonProperty("target") targetParam: DataParam,
                      @JsonProperty("evaluateRule") evaluateRuleParam: EvaluateRuleParam
                    ) extends Param {

}
