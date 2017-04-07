package org.apache.griffin.measure.batch.connector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.{Success, Try}

case class HiveDataConnector(sqlContext: SQLContext, config: Map[String, Any]) extends DataConnector {

  val Database = "database"
  val TableName = "table.name"
  val Partitions = "partitions"

  val database = config.getOrElse(Database, "").toString
  val tableName = config.getOrElse(TableName, "").toString
  val partitionsString = config.getOrElse(Partitions, "").toString

  val concreteTableName = if (dbPrefix) s"${database}.${tableName}" else tableName
  val partitions = partitionsString.split(";").map(s => s.split(",").map(_.trim))

  private def dbPrefix(): Boolean = {
    !database.isEmpty && !database.equals("default")
  }

  def available(): Boolean = {
    (!concreteTableName.isEmpty) && {
      Try {
        sqlContext.sql(tableExistsSql).collect.size
      } match {
        case Success(s) => s > 0
        case _ => false
      }
    }
  }

  def metaData(): Try[Iterable[(String, String)]] = {
    Try {
      val originRows = sqlContext.sql(metaDataSql).map(r => (r.getString(0), r.getString(1))).collect
      val partitionPos: Int = originRows.indexWhere(pair => pair._1.startsWith("# "))
      if (partitionPos < 0) originRows
      else originRows.take(partitionPos)
    }
  }

//  def data(): Try[RDD[Map[String, Any]]] = {
//    Try {
//      val df: RDD[Row] = sqlContext.sql(dataSql).map(r=>r)
//
//      // fixme: get data
//
//    }
//  }

  private def tableExistsSql(): String = {
    s"SHOW TABLES LIKE '${concreteTableName}'"
  }

  private def metaDataSql(): String = {
    s"DESCRIBE ${concreteTableName}"
  }

  private def dataSql(): String = {
    val clauses = partitions.map { prtn =>
      val cls = prtn.mkString(" AND ")
      if (cls.isEmpty) s"SELECT * FROM ${concreteTableName}"
      else s"SELECT * FROM ${concreteTableName} WHERE ${cls}"
    }
    clauses.mkString(" UNION ALL ")
  }

}
