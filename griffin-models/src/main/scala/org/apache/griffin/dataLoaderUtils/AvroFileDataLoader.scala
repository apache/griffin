package org.apache.griffin.dataLoaderUtils

import org.apache.griffin.accuracy.AccuracyConfEntity
import org.apache.griffin.validility.ValidityConfEntity
import org.apache.spark.sql.{DataFrame, SQLContext}

import com.databricks.spark.avro._

case class AvroFileDataLoader (sqlc: SQLContext, dataFilePath: String) extends DataLoader(sqlc) {
  final val dataAvroFileSuffix = ".avro"

  def getAccuDataFrame(accu: AccuracyConfEntity) : (DataFrame, DataFrame) = {
    val srcDf = loadDataFile(dataFilePath + accu.source)
    val tgtDf = loadDataFile(dataFilePath + accu.target)
    (srcDf, tgtDf)
  }

  def getValiDataFrame(vali: ValidityConfEntity) : DataFrame = {
    val srcDf = loadDataFile(dataFilePath + vali.dataSet)
    srcDf
  }

  def loadDataFile(file: String) = {
    val fpath = if (file.endsWith(dataAvroFileSuffix)) file else file + dataAvroFileSuffix
    sqlContext.read.avro(fpath)
  }
}
