package org.apache.griffin.dataLoaderUtils

import org.apache.spark.sql.SQLContext

object DataLoaderFactory {
  final val hive = "hive"
  final val avro = "avro"
  final val csv = "csv"

  def getDataLoader(sqlc: SQLContext, tp: String, dataFilePath: String = ""): DataLoader = {
    tp match {
      case this.hive => HiveDataLoader(sqlc)
      case this.avro => new AvroFileDataLoader(sqlc, dataFilePath)
      case this.csv => CsvFileDataLoader(sqlc, dataFilePath)
      case _ => null
    }
  }
}
