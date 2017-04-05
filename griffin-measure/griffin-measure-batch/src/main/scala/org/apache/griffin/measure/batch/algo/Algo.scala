package org.apache.griffin.measure.batch.algo

import org.apache.griffin.measure.batch.config.params.env._
import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.log.Loggable

import scala.util.Try

trait Algo extends Loggable with Serializable {

  val envParam: EnvParam
  val userParam: UserParam

  def run(): Try[_]

}
