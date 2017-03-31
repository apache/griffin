package org.apache.griffin.measure.batch.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class AllParam( @JsonProperty("abc") abc: String
                   ) extends ConfigParam {

}
