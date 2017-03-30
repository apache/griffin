package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class DataAssetParam( @JsonProperty("kafka.config") kafkaConfig: Map[String, String],
                           @JsonProperty("topics") topics: String,
                           @JsonProperty("pre.process") prepParam: PrepParam,
                           @JsonProperty("dump.config") dumpConfigParam: DumpParam,
                           @JsonProperty("schema") schema: List[SchemaFieldParam]
                         ) extends ConfigParam {

}
