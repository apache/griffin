package org.apache.griffin.dataLoaderUtils

import org.apache.griffin.validility.ValidityConfEntity
import org.apache.griffin.accuracy.AccuracyConfEntity
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class DataLoader(val sqlContext: SQLContext) extends Logging {
  def getAccuDataFrame(accu: AccuracyConfEntity) : (DataFrame, DataFrame)
  def getValiDataFrame(vali: ValidityConfEntity) : DataFrame
}
