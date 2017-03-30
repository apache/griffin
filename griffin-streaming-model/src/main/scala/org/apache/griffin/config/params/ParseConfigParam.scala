package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class ParseConfigParam( @JsonProperty("out.schema") schema: List[SchemaFieldParam]
                           ) extends ConfigParam {

}
