package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class DumpParam( @JsonProperty("dir") dir: String,
//                      @JsonProperty("field.separator") fieldSep: String,
//                      @JsonProperty("line.separator") lineSep: String,
                      @JsonProperty("table.name") tableName: String,
                      @JsonProperty("schema") schema: List[SchemaFieldParam]
                    ) extends ConfigParam {

}
