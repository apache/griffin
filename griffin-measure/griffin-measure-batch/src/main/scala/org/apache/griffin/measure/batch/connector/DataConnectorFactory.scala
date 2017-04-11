package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.config.params.user.ConnectorParam
import org.apache.griffin.measure.batch.dsl.expr._
import org.apache.spark.sql.SQLContext

import scala.util.Try

object DataConnectorFactory {

  val HiveRegex = """^(?i)hive$""".r
  val AvroRegex = """^(?i)avro$""".r

  def getDataConnector(sqlContext: SQLContext,
                       connectorParam: ConnectorParam,
                       keyExprs: Seq[DataExpr],
                       dataExprs: Iterable[DataExpr]
                      ): Try[DataConnector] = {
    val conType = connectorParam.conType
    val version = connectorParam.version
    Try {
      conType match {
        case HiveRegex() => HiveDataConnector(sqlContext, connectorParam.config, keyExprs, dataExprs)
        case AvroRegex() => AvroDataConnector(sqlContext, connectorParam.config, keyExprs, dataExprs)
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

}
