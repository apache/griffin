package org.apache.griffin.measure.config.params.env

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.griffin.measure.config.params.Param

/**
  * Created by xiaoqiu.duan on 2017/10/23.
  */
@JsonInclude(Include.NON_NULL)
case class EmailParam(@JsonProperty("host") host: String,
                       @JsonProperty("mail") mail: String,
                       @JsonProperty("user") usr: String,
                       @JsonProperty("password") pwd: String
                     ) extends Param {

}
