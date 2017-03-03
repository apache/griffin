package org.apache.griffin.dataLoaderUtils

import java.io.File

import org.apache.griffin.accuracy.AccuracyConfEntity
import org.apache.griffin.util.{DataTypeUtils, PartitionUtils}
import org.apache.griffin.validility.ValidityConfEntity
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

case class CsvFileDataLoader(sqlc: SQLContext, dataFilePath: String) extends DataLoader(sqlc) {
  final val dataDescFileSuffix = "_type"

  def getAccuDataFrame(accu: AccuracyConfEntity) : (DataFrame, DataFrame) = {
    val srcDf = loadDataFile(dataFilePath + accu.source)
    val tgtDf = loadDataFile(dataFilePath + accu.target)
    (srcDf, tgtDf)
  }

  def getValiDataFrame(vali: ValidityConfEntity) : DataFrame = {
    val srcDf = loadDataFile(dataFilePath + vali.dataSet)
    srcDf
  }

  def loadDataFile(file: String, fmt: String = "com.databricks.spark.csv") = {
    val reader: DataFrameReader = sqlContext.read
      .format(fmt)
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("delimiter", ",")

    val typeFile = file + dataDescFileSuffix
    val typeExist = new File(typeFile).exists()
    val schemaReader = if (typeExist) {
      val types = reader.load(typeFile).collect.map( r => (r.getString(0), r.getString(1)) )
      val fields = types.map(kt => StructField(kt._1, DataTypeUtils.str2DataType(kt._2)))
      val customSchema = StructType(fields)
      reader.schema(customSchema)
    } else {
      reader.option("inferSchema", "true")
    }

    schemaReader.load(file)
  }
}
