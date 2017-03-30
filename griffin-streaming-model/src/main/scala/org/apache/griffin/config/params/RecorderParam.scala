package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class RecorderParam( @JsonProperty("types") types: List[String],
                          @JsonProperty("metric.name") metricName: String,
                          @JsonProperty("config") config: Map[String, String]
                        ) extends ConfigParam {

}
