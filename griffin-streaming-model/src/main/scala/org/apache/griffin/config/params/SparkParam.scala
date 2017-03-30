package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class SparkParam( @JsonProperty("app.name") appName: String,
                       @JsonProperty("log.level") logLevel: String,
                       @JsonProperty("config") config: Map[String, String],
                       @JsonProperty("streaming.checkpoint.dir") cpDir: String,
                       @JsonProperty("streaming.batch.interval.seconds") batchInterval: Int,
                       @JsonProperty("streaming.sample.interval.seconds") sampleInterval: Int
                     ) extends ConfigParam {

}
