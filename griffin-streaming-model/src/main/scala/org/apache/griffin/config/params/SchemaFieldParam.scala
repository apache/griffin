package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class SchemaFieldParam( @JsonProperty("name") name: String,
                             @JsonProperty("type") fieldType: String,
                             @JsonProperty("default.value") defaultValue: String,
                             @JsonProperty("extract.steps") extractSteps: List[String]
                           ) extends ConfigParam {

  override def equals(x: Any): Boolean = {
    if (x.isInstanceOf[SchemaFieldParam]) {
      val s = x.asInstanceOf[SchemaFieldParam]
      s.name == this.name
    } else false
  }

}
