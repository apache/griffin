package org.apache.griffin.measure.batch.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.batch.config.params.env._
import org.apache.griffin.measure.batch.config.params.user._

// simply composite of env and user params, for convenient usage
@JsonInclude(Include.NON_NULL)
case class AllParam( @JsonProperty("env") envParam: EnvParam,
                     @JsonProperty("user") userParam: UserParam
                   ) extends Param {

}
