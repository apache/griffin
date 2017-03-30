package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class PrepParam( @JsonProperty("sample") sampleParam: SampleParam,
                      @JsonProperty("parse") parseParam: ParseParam
                    ) extends ConfigParam {

}
