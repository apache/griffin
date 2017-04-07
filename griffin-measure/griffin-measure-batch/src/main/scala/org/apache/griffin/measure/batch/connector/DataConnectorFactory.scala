package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.config.params.user.ConnectorParam
import org.apache.spark.sql.SQLContext

object DataConnectorFactory {

  val HiveRegex = """^(?i)hive$""".r

  def getDataConnector(sqlContext: SQLContext, connectorParam: ConnectorParam): Option[DataConnector] = {
    val conType = connectorParam.conType
    val version = connectorParam.version
    conType match {
      case HiveRegex() => {
        Some(HiveDataConnector(sqlContext, connectorParam.config))
      }
      case _ => None
    }
  }

}
