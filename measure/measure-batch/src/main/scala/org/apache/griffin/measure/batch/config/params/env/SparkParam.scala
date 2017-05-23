package org.apache.griffin.measure.batch.config.params.env

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.batch.config.params.Param

@JsonInclude(Include.NON_NULL)
case class SparkParam( @JsonProperty("log.level") logLevel: String,
                       @JsonProperty("checkpoint.dir") cpDir: String,
                       @JsonProperty("config") config: Map[String, Any]
                     ) extends Param {

}
