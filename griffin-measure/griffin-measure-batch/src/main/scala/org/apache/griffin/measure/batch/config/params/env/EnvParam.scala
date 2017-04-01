package org.apache.griffin.measure.batch.config.params.env

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.griffin.measure.batch.config.params.Param

@JsonInclude(Include.NON_NULL)
case class EnvParam( @JsonProperty("spark") sparkParam: SparkParam,
                     @JsonProperty("persist") persistParams: List[PersistParam],
                     @JsonProperty("cleaner") cleanerParam: CleanerParam
                   ) extends Param {

}
