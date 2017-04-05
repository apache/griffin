package org.apache.griffin.measure.batch.algo

import org.apache.griffin.measure.batch.config.params.AllParam

import scala.util.Try


case class BatchAccuracyAlgo(allParam: AllParam) extends AccuracyAlgo {
  val envParam = allParam.envParam
  val userParam = allParam.userParam

  def run(): Try[_] = {
    Try {

    }
  }

}
