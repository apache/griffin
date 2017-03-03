package org.apache.griffin.dataLoaderUtils

import org.apache.griffin.accuracy.AccuracyConfEntity
import org.apache.griffin.util.PartitionUtils
import org.apache.griffin.validility.ValidityConfEntity
import org.apache.spark.sql.{DataFrame, SQLContext}

case class HiveDataLoader(sqlc: SQLContext) extends DataLoader(sqlc) {

  def getAccuDataFrame(accu: AccuracyConfEntity) : (DataFrame, DataFrame) = {
    val srcDf = sqlContext.sql(PartitionUtils.generateSourceSQLClause(accu.source, accu.srcPartitions))
    val tgtDf = sqlContext.sql(PartitionUtils.generateTargetSQLClause(accu.target, accu.tgtPartitions))
    (srcDf, tgtDf)
  }

  def getValiDataFrame(vali: ValidityConfEntity) : DataFrame = {
    val srcDf = sqlContext.sql(PartitionUtils.generateSourceSQLClause(vali.dataSet, vali.timePartitions))
    srcDf
  }
}
