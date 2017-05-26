package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.rule.RuleExprs
import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.sql.SQLContext

import scala.util.Try

object DataConnectorFactory {

  val HiveRegex = """^(?i)hive$""".r
  val AvroRegex = """^(?i)avro$""".r

  def getDataConnector(sqlContext: SQLContext,
                       dataConnectorParam: DataConnectorParam,
                       ruleExprs: RuleExprs,
                       globalFinalCacheMap: Map[String, Any]
                      ): Try[DataConnector] = {
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    Try {
      conType match {
        case HiveRegex() => HiveDataConnector(sqlContext, dataConnectorParam.config, ruleExprs, globalFinalCacheMap)
        case AvroRegex() => AvroDataConnector(sqlContext, dataConnectorParam.config, ruleExprs, globalFinalCacheMap)
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

}
