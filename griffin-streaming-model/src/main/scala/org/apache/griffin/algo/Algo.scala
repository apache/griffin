package org.apache.griffin.algo

import org.apache.griffin.config.params._

trait Algo extends Serializable {

  val sparkParam: SparkParam

  def run(): Unit

}
