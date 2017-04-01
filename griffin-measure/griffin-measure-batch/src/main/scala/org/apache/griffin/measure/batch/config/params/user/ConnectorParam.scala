package org.apache.griffin.measure.batch.config.params.user

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.batch.config.params.Param

@JsonInclude(Include.NON_NULL)
case class ConnectorParam( @JsonProperty("type") conType: String,
                           @JsonProperty("version") version: String,
                           @JsonProperty("config") config: String
                         ) extends Param {

}
