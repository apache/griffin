package org.apache.griffin.algo

import org.apache.griffin.config.params._


trait AccuracyAlgo extends Algo {

  val sourceDataParam: DataAssetParam
  val targetDataParam: DataAssetParam

}
