package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class MappingParam( @JsonProperty("source.name") sourceName: String,
                         @JsonProperty("target.name") targetName: String,
                         @JsonProperty("isPK") isPK: Boolean
                         ) extends ConfigParam {

}
