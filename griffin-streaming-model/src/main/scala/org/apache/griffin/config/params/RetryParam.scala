package org.apache.griffin.config.params

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class RetryParam( @JsonProperty("need.retry") needRetry: Boolean,
                       @JsonProperty("next.retry.seconds") nextRetryInterval: Long,
                       @JsonProperty("interval.seconds") interval: Long
                     ) extends ConfigParam {

}
